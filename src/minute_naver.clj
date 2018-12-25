(ns minute-naver
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.core.async :as a :refer [>!! >! <! <!!]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as r]))

(def date (java.time.LocalDate/now))

(def redis-uri (or (env :redis-uri) "redis://192.168.0.3:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(def influxdb-uri (or (env :influxdb-uri) "http://192.168.0.3:8086"))

(defn check-influxdb []
  (= 204 (-> (str influxdb-uri "/ping")
             client/get
             :status)))

(def symbols
  (for [symbol (shuffle (redis (r/keys "stock:*")))]
    (s/replace symbol "stock:" "")))

(defn generate-url
  [symbol page]
  (let [d (.format date (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd"))]
    (format "http://finance.naver.com/item/sise_time.nhn?code=%s&page=%s&thistime=%s180000"
            symbol page d)))

(defn download [url]
  (let [options {:as             :byte-array
                 :socket-timeout 3000
                 :retry-handler  (fn [ex try-count http-context]
                                   (log/error "Error:" ex)
                                   (if (> try-count 4) false true))}]
    (-> url
        (client/get options)
        :body
        (String. "euc-kr"))))

(defn parse-last-page [lines]
  (try
    (->> lines
         (filter #(s/includes? % "맨뒤"))
         first
         (re-find #"page=(\d+)")
         last
         read-string)
    (catch NullPointerException e
      1)))

(defn parse-rows [lines]
  (transduce
    (comp
      (filter
        #(or (s/includes? % "<td class=\"num\"><span class=\"tah p11\">")
             (s/includes? % "<td align=\"center\"><span class=\"tah p10 gray03\">")))
      (map #(s/replace % "," ""))
      (map #(re-find #"<span class=\".+\">(.+)</span>" %))
      (map last)
      (filter some?)
      (partition-all 6))
    conj
    lines))

(defn get-line-protocol
  [symbol rows]
  (for [[hhmm price _ _ _ volume] rows
        :let [timestamp (-> (format "%sT%s:00Z" date hhmm)
                            (java.time.Instant/parse)
                            (.getEpochSecond))]]
    (format
      "minute,symbol=%s price=%si,volume=%si %s"
      symbol
      price
      volume
      timestamp)))

(defn save-influxdb
  [rows]
  (client/post (str influxdb-uri "/write?db=history&precision=s")
               {:async? true
                :body   (s/join "\n" rows)}
               (fn [res])
               (fn [err] (log/error (.getMessage err))))
  nil)

(defn save-disk
  [symbol rows]
  (let [datedir  (.format date (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd"))
        filepath (format "../data.minute/%s/%s.txt" datedir symbol)]
    (with-open [w (io/writer filepath :append true)]
      (binding [*out* w]
        (doseq [[hhmm price _ _ _ volume] rows]
          (println
            (format
              "minute,symbol=%s price=%si,volume=%si %s"
              symbol
              price
              volume
              (-> (format "%sT%s:00Z" date hhmm)
                  (java.time.Instant/parse)
                  (.getEpochSecond))))))))
  (log/trace (str "Saved: " symbol " " (count rows))))

(defn process
  [symbol]
  (let [url       (generate-url symbol 1)
        html      (download url)
        lines     (s/split-lines html)
        last-page (parse-last-page lines)]
    (loop [total-rows 0
           page       1
           rows       (parse-rows lines)]
      (if (<= page last-page)
        (do
          (when (seq rows)
            (-> (get-line-protocol symbol rows)
                (save-influxdb)))
          (recur
            (+ total-rows (count rows))
            (inc page)
            (-> (generate-url symbol (inc page))
                download
                s/split-lines
                parse-rows)))
        (do
          (log/info (format "[%s] %,d rows saved" symbol total-rows))
          {:symbol symbol :rows total-rows})))))

(defn -main []
  (comment (io/make-parents (format "%s/file" save-dir)))
  (if (check-influxdb)
    (log/info "InfluxDB is ready" influxdb-uri)
    (throw (Exception. (str "InfluxDB is not running " influxdb-uri))))

  (time
    (<!!
      (a/pipeline-blocking
        (* 2 (.availableProcessors (Runtime/getRuntime)))
        (doto (a/chan) (a/close!))
        (map process)
        (a/to-chan symbols)
        true
        (fn [err] (log/error (.getMessage err))))))
  )


(comment
  (-main)

  (def date (java.time.LocalDate/now))
  (def date (.minusDays date 1))

  (def symbols ["015760" "000020"])
  (def symbols ["131290" "200250"])

  (parse-last-page (s/split-lines -html))

  (def -symbol (first symbols))
  (def -url (generate-url -symbol 1))
  (def -html (download -url))
  (def -rows (parse-rows (s/split-lines -html)))
  (def -lps (get-line-protocol -symbol -rows))
  (save-influxdb -lps)


  (redis (r/ping))

  (a/go (a/>! output {:symbol "015760" :rows 420}))
  (a/go (println (a/<! output)))
  )

