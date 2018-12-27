(ns price-day
  (:require [clj-http.client :as client]
            [clojure.string :as s]
            [clojure.core.async :as a :refer [>!! >! <! <!!]]
            [environ.core :refer [env]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as r]
            [clojure.string :as str]))

(def redis-uri (or (env :redis-uri) "redis://localhost:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(def influxdb-uri (or (env :influxdb-uri) "http://192.168.0.3:8086"))

(defn get-symbols []
  (for [symbol (shuffle (redis (r/keys "stock:*")))]
    (s/replace symbol "stock:" "")))

(defn get-url
  ([symbol] (get-url symbol 1))
  ([symbol page]
   (format "http://finance.naver.com/item/sise_day.nhn?code=%s&page=%s" symbol page)))

(defn download
  ([url]
   (let [options {:as             :byte-array
                  :socket-timeout 3000
                  :retry-handler  (fn [ex try-count http-context]
                                    (log/error (.getMessage ex))
                                    (if (> try-count 5) false true))}]
     (-> url
         (client/get options)
         :body
         (String. "euc-kr")))))

(defn parse-last-page
  [lines]
  (try
    (->> lines
         (filter #(s/includes? % "맨뒤"))
         first
         (re-find #"page=(\d+)")
         last
         read-string)
    (catch NullPointerException e 1)))

(defn parse
  "parse html and return as follows:
   ((\"2018.04.30\" \"37450\" \"37500\" \"37750\" \"36650\" \"2820836\")) "
  [lines]
  (transduce
    (comp
      (filter
        #(or (s/includes? % "<td align=\"center\"><span class=\"tah p10 gray03\">")
             (s/includes? % "<td class=\"num\"><span class=\"tah p11\">")))
      (map #(s/replace % "," ""))
      (map #(re-find #"<span class=\".+\">(.+)</span>" %))
      (map last)
      (filter some?)
      (partition-all 6))
    conj
    lines))

(defn convert
  [symbol rows]
  (for [[date close open high low volume] rows
        :let [timestamp-hour (-> (s/replace date "." "-")
                                 (str "T16:00:00Z")
                                 (java.time.Instant/parse)
                                 (.getEpochSecond)
                                 (/ 3600))]]
    (format
      "day,symbol=%s close=%si,open=%si,high=%si,low=%si,volume=%si %s"
      symbol close open high low volume timestamp-hour)))

(defn save
  [rows]
  (client/post (str influxdb-uri "/write?db=history&precision=h")
               {:async? true
                :body   (s/join "\n" rows)}
               (fn [res])
               (fn [err] (log/error (.getMessage err))))
  (count rows))

(defn process-recent
  [symbol]
  (let [rows (->> symbol
                  get-url
                  download
                  s/split-lines
                  parse
                  (convert symbol)
                  save)]
    (log/info (format "[%s] %,d rows saved" symbol rows))
    {:symbol symbol :rows rows}))

(defn process-all
  [symbol]
  (let [url       (get-url symbol 1)
        html      (download url)
        lines     (s/split-lines html)
        last-page (parse-last-page lines)]
    (loop [total-rows 0
           page       1
           rows       (parse lines)]
      (if (<= page last-page)
        (do
          (when (seq rows)
            (-> (convert symbol rows)
                (save)))
          (recur
            (+ total-rows (count rows))
            (inc page)
            (-> (get-url symbol (inc page))
                download
                s/split-lines
                parse)))
        (do
          (log/info (format "[%s] %,d rows saved" symbol total-rows))
          {:symbol symbol :rows total-rows})))))

(defn check-influxdb [uri]
  (= 204 (-> (str uri "/ping")
             client/get
             :status)))

(defn -main []
  (log/info "Interday Prices (from NAVER)")

  (if (check-influxdb influxdb-uri)
    (log/info "InfluxDB is ready" influxdb-uri)
    (throw (Exception. (str "InfluxDB is not running " influxdb-uri))))

  (time
    (<!!
      (a/pipeline-blocking
        (* 4 (.availableProcessors (Runtime/getRuntime)))
        (doto (a/chan) (a/close!))
        (map process-recent)
        ;(map process-all)
        (a/to-chan (get-symbols))
        true
        (fn [err] (log/error (.getMessage err)))))))

(comment
  (def page 1)
  (def -symbol "015760")
  (def -url (get-url -symbol))
  (def -html (download -url))
  (parse-last-page -html)
  (def -lines (s/split-lines -html))
  (def -rows (parse -lines))
  (def -lps (convert -symbol -rows))
  (save -lps)
  (process-recent "015760")
  (defn get-symbols [] ["015760" "000020"])
  (-main))
