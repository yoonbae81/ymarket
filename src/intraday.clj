(ns intraday
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(def ETFs (set (redis (r/smembers "etf"))))
(def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))
(def basedir (str "../data.intraday/" date))
(def lp-format "intraday,symbol=%s price=%s,volume=%s %s")

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

(defn post [url options]
  (let [opts (merge options
                    {:socket-timeout 180000
                     :conn-timeout   3000})]
    (loop [retry 1]
      (if-let [res (try (client/post url opts)
                        (catch Exception e
                          (log/error "Failed")
                          (when (zero? retry)
                            {:body "" :length 0 :request-timnde 9999})))]
        res
        (recur (dec retry))))))

(defn get-otp [symbol]
  (log/trace "Requesting OTP")
  (let [fullcode (redis (r/hget symbol "fullcode"))
        url      (if (contains? ETFs symbol)
                   "MKD/08/0801/08010700/mkd08010700_02"
                   "MKD/04/0402/04020100/mkd04020100t3_01")
        res      (post "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
                       {:form-params {:name      "fileDown"
                                      :filetype  "csv"
                                      :isu_cd    fullcode
                                      :url       url
                                      :aceString "1"}})]
    (log/trace "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [symbol otp]
  (let [res (post "http://file.krx.co.kr/download.jspx"
                  {:form-params {:code otp}})]
    (log/trace "Received" symbol (format "%,d" (:length res)) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn parse [symbol data]
  (let [idx (if (contains? ETFs symbol)
              {:count 7 :time 0 :price 1 :volume 6}
              {:count 6 :time 1 :price 2 :volume 4})]
    (for [line (rest (str/split-lines data))
          :let [row (-> line
                        (str/replace #"(\d),(\d)" "$1$2")
                        (str/replace #"\"" "")
                        (str/split #","))]
          :when (= (count row) (:count idx))]
      (let [time   (get row (:time idx))
            price  (get row (:price idx))
            volume (get row (:volume idx))]
        {:time time :price price :volume volume}))))

(defn save [symbol rows]
  (let [symbol   (str/replace symbol "stock:" "")
        filepath (str basedir "/" symbol ".txt")]
    (clojure.java.io/make-parents filepath)
    (with-open [w (clojure.java.io/writer filepath)]
      (binding [*out* w]
        (println "# DDL")
        (println "CREATE DATABASE KRX")
        (println "# DML")
        (println "# CONTEXT-DATABASE: KRX")

        (doseq [row rows
                :let [time      (:time row)
                      millisec  (next-value)
                      timestamp (get-timestamp date time millisec)
                      price     (:price row)
                      volume    (:volume row)]]
          (println (format lp-format
                           symbol price volume timestamp)))))))

(defn run [symbol]
  (log/info "Downloading" (redis (r/hget symbol "name")) symbol)
  (->> (get-otp symbol)
       (fetch symbol)
       (parse symbol)
       (save symbol)))

(defn -main []
  (log/info "Intraday Prices (from KRX)")
  (doseq [symbol (redis (r/keys "stock:*"))
          :let [stock (redis (r/hgetall* symbol))]]
    (run symbol)))

(comment
  (redis (r/hgetall* symbol))
  (def symbol "stock:015760")
  (def symbol "stock:005930")
  (def symbol "stock:112240")
  (def symbol "stock:069500")                               ; KODEX200                              ;
  ; KODEX KTOP30 makes EOFException
  (def symbol "stock:229720")
  ; KODEX 레버리지 never succeed
  (def symbol "stock:122630")
  (run symbol)
  (def otp (get-otp symbol))
  (def data (fetch symbol otp))
  (def line (first (rest (str/split-lines data))))
  (def line (last (str/split-lines data)))
  (def rows (parse symbol data))
  (save symbol rows)
  )
