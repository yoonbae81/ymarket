(ns price-day
  (:require [clj-http.client :as http]
            [clojure.string :as str]
            [environ.core :refer [env]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as r]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn fetch
  ([symbol] (fetch symbol 1))
  ([symbol page]
   (log/debug (format "Fetching %s (page: %,d)" symbol page))
   (let [url  (format "http://finance.naver.com/item/sise_day.nhn?code=%s&page=%s" symbol page)
         res  (http/get url {:as             :byte-array
                             :socket-timeout 3000
                             :conn-timeout   3000
                             :retry-handler  (fn [ex try-count http-context]
                                               (log/error ex)
                                               (if (> try-count 5) false true))})
         body (:body res)]
     (String. body "euc-kr"))))

(defn parse
  "parse html and return as follows:
   ((\"2018.04.30\" \"37450\" \"37500\" \"37750\" \"36650\" \"2820836\")) "
  [data]
  (when (str/includes? data "맨뒤")
    (->> data
         str/split-lines
         (filter
           #(or (str/includes? % "<td align=\"center\"><span class=\"tah p10 gray03\">")
                (str/includes? % "<td class=\"num\"><span class=\"tah p11\">")))
         (map #(->> (str/replace % "," "")
                    (re-find #"<span class=\".+\">(.+)</span>")
                    last))
         (filter some?)
         (partition 6))))

(defn- timestamp
  [date]
  (-> (str/replace date "." "-")
      (str "T00:00:00Z")
      (java.time.Instant/parse)
      (.getEpochSecond)))

(defn convert [symbol data]
  (for [row data
        :when (not-empty row)]
    (apply format
           "day,symbol=%s close=%si,open=%si,high=%si,low=%si,volume=%si %s"
           (concat (conj (rest row) symbol) [(timestamp (first row))]))))

(defn save [data]
  (let [res (http/post
              "http://127.0.0.1:8086/write?db=price&precision=s"
              {:body           (str/join "\n" data)
               :socket-timeout 3000
               :conn-timeout   3000
               :retry-handler  (fn [ex try-count http-context]
                                 (log/error ex)
                                 (if (> try-count 4) false true))})]
    (when (= (:status res) 204)
      (log/trace (count data) "records saved")))
  (count data))

(defn run
  ([symbol] (run symbol 1))
  ([symbol page]
   (->> (fetch symbol page)
        parse
        (convert symbol)
        save)))

(defn run-all [symbol]
  (loop [acc 0 page 1]
    (let [days (run symbol page)]
      (if (not= 10 days)
        (let [total (+ acc days)]
          (redis (r/sadd "price-done" (str "stock:" symbol)))
          (log/info (format "Fetched %s (%,d records)" symbol total))
          total)
        (recur (+ acc days) (inc page))))))

(defn -main []
  (log/info "Creating a database on InfluxDB")
  (http/post "http://127.0.0.1:8086/query"
             {:form-params {:q "CREATE DATABASE price"}})
  (log/info "Fetching daily prices from NAVER")
  (let [days  (for [symbol (shuffle (redis (r/keys "stock:*")))
                    :when (= 0 (redis (r/sismember "price-done" symbol)))]
                (run-all (str/replace symbol "stock:" "")))
        total (reduce + days)]
    (log/info "Done:" total "records saved from" (count days) "symbols")))

(comment
  (def page 1)
  (def symbol "015761")
  (def fetched (fetch "01576A0" 1))
  (def parsed (parse fetched))
  (def converted (convert symbol parsed))
  (def res (save converted))
  (run "015760")
  (run-all "015760")
  (-main))
