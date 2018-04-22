(ns intraday-etf
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))
(def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))

(defn get-filepath [symbol]
  (format "../data.intraday/%s/%s.txt"
          date
          (str/replace symbol "stock:" "")))

(defn post [url options]
  (loop [retry 4]
    (if-let [res (try (client/post url options)
                      (catch Exception e
                        (log/error "Requesting Timeout")))]
      (if (= 200 (:status res))
        res
        (log/error (format "Response Error (Status:%s)" (:status res))))
      (recur (dec retry)))))

(defn fetch
  [symbol]
  (log/debug "Requesting" symbol)
  (let [fullcode (redis (r/hget symbol "fullcode"))
        otp      (client/get "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD/08/0801/08010700/mkd08010700_02&name=form")
        params   {:code      (:body otp)
                  :isu_cd    fullcode
                  :acsString "1"}
        res      (post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                       {:form-params    params
                        :socket-timeout 180000
                        :conn-timeout   3000})]
    (when res (log/trace "Received" (:length res) "bytes in" (:request-time res) "ms"))
    (:body res)))

(defn parse [data]
  (for [row (:block1 (json/read-json data))
        :let [time   (:hms row)
              price  (:isu_cur_pr row)
              volume (:isu_tr_cnt row)]]
    {:time time :price price :volume volume}))

(def counter (atom 1000))
(defn next-value []
  (when (zero? @counter) (reset! counter 1000))
  (swap! counter dec))

(defn get-timestamp [date time millisec]
  ; (t.EpochMilli (java.time.Instant/parse "2018-04-18T12:34:56Z"))
  (-> (str date "T" time "Z")
      (java.time.Instant/parse)
      (.toEpochMilli)
      ; +09:00 to UTC
      ; (- 32400000)
      (+ millisec)))

(defn save [symbol rows]
  (with-open [w (io/writer (get-filepath symbol) :append true)]
    (binding [*out* w]
      (doseq [row rows
              :let [time      (:time row)
                    millisec  (next-value)
                    timestamp (get-timestamp date time millisec)
                    price     (:price row)
                    volume    (:volume row)]]
        (println
          (format "intraday,symbol=%s price=%s,volume=%s %s"
                  (str/replace symbol "stock:" "")
                  price
                  volume
                  timestamp)))))
  (when (seq rows) (log/debug (format "Saved %,d records of %s" (count rows) symbol)))
  rows)

(defn run [symbol]
  (io/make-parents (get-filepath symbol))
  (with-open [w (io/writer (get-filepath symbol))]
    (binding [*out* w]
      (println "# DDL")
      (println "CREATE DATABASE KRX")
      (println "# DML")
      (println "# CONTEXT-DATABASE: KRX")))

  (->> (fetch symbol)
       (parse)
       (save symbol)
       (count)))

(defn -main []
  (doseq [symbol (shuffle (redis (r/smembers "etf")))]
    (if (.exists (io/as-file (get-filepath symbol)))
      (log/info "File exists (skip)" symbol)
      (run symbol))))

(comment
  ; ETF 파워코스피100
  (run "stock:140950"))

