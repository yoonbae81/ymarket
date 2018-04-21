(ns yquant.stock.price.intraday
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(def etf (set (redis (r/smembers "etf"))))

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

(defn get-otp [url, fullcode]
  (log/debug "Requesting OTP")
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name      "fileDown"
                             :filetype  "csv"
                             :url       url
                             :isu_cd    fullcode
                             :aceString "1"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/debug "Downloading Data")
  (let [res (client/post "http://file.krx.co.kr/download.jspx"
                         {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn parse [data]
  (log/info "Saving to Redis")
  ; ETF
  ; 주식 ["﻿총곗수" "시분초" "종목현재가" "대비" "거래량" "거래대금"]
  (doseq [row (rest (csv/read-csv data))]
    ))

(def urls
  {:etf        "MKD/08/0801/08010700/mkd08010700_02"
   :securities "MKD/04/0402/04020100/mkd04020100t3_01"})

(defn single [symbol]
  (contains? etf (str "stock:" "091160"))
  )

(defn -main []
  (log/info "일간 주가변동 (from KRX)")

  (def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))

  (-> (get-otp)
      (fetch)
      (parse)))

(comment
  (def fullcode "KR7032681009")
  (def stocks (redis (r/keys "stock:*")))
  (def stock (redis (r/hgetall* (first stocks))))
  (def etf (redis (r/smembers "etf")))

  (doseq [symbol (redis (r/keys "stock:*"))
          :let [stock (redis (r/hgetall* symbol))]]
    (println stock))

  (def res
    (-> (get-otp "MKD/04/0402/04020100/mkd04020100t3_01"
                 "KR7032681009")
        (fetch)))
  (doseq [row (rest (csv/read-csv res))
          :let [time   (get row 1)
                price  (str/replace (get row 2) "," "")
                volume (str/replace (get row 4) "," "")]]
    (println time price volume))

  )
