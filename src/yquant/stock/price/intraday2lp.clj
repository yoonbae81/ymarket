(ns yquant.stock.price.intraday2lp
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(def counter (atom -1))
(defn next-value
  []
  (when (< 999 @counter) (reset! counter -1))
  (swap! counter inc))

(defn get-timestamp [date time millisec]
  ; (t.EpochMilli (java.time.Instant/parse "2018-04-18T12:34:56Z"))
  (-> (str date "T" time "Z")
      (java.time.Instant/parse)
      (.toEpochMilli)
      ;      (- 32400000)                                          ; +09:00 to UTC
      (+ millisec)))

(defn -main [filepath]
  (def date (re-find #"\d{4}-\d{2}\-\d{2}", filepath))
  (def line-protocol "intraday,symbol=%s price=%s,volume=%s %s")

  (println "# DDL")
  (println "CREATE DATABASE KRX")
  (println "# DML")
  (println "# CONTEXT-DATABASE: KRX")

  (with-open [reader (io/reader filepath)]
    (doseq [row (csv/read-csv reader :separator \space)]
      (let [time      (get row 0)
            millisec  (next-value)
            timestamp (get-timestamp date time millisec)
            symbol    (get row 1)
            price     (get row 2)
            volume    (get row 3)]
        (println (format line-protocol
                         symbol price volume timestamp))))))
