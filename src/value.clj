(ns value
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.data.csv :as csv]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clojure.string :as str]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn get-otp [market]
  (log/info "Requesting OTP")
  (let [date (.format (java.text.SimpleDateFormat. "yyyyMMdd") (new java.util.Date))
        res  (client/post
               "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
               {:form-params {:name         "fileDown"
                              :filetype     "csv"
                              :url          "MKD/04/0404/04040200/mkd04040200_01"
                              :schdate      date
                              :market_gubun market
                              :sect_tp_cd   "ALL"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/debug "Downloading Data")
  (let [res
        (client/post "http://file.krx.co.kr/download.jspx"
                     {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

;﻿0순위,1종목코드,2종목명,3현재가,4대비,5등락률,6거래량,7거래대금,8시가,9고가,10저가,11시가총액,12시가총액비중(%),13상장주식수(천주),14외국인 보유주식수,15외국인 지분율(%),16총카운트
;1,091990,셀트리온헬스케어,"101,000","-2,500","-2.4","772,380","79,255,253,800","104,900","104,900","100,900","13,889,976,520,000","5.00","137,524,520",,,1272
;2,215600,신라젠,"107,100","2,000","1.9","2,290,914","247,920,541,500","106,100","110,700","104,800","7,303,497,717,600","2.63","68,193,256",,,

(defn save [data]
  (log/info "Saving to Redis")
  (doseq [row (rest (csv/read-csv data))                    ; (rest) for skipping the header
          :let [symbol (get row 1)
                value  (read-string (str/replace (get row 11) "," ""))]]
    (log/debug symbol value)
    (redis (r/hmset (str "stock:" symbol)
                    "value" value))))

(defn -main []
  (log/info "시가총액 (from KRX)")
  (doseq [code '("STK" "KSQ")]
    (-> code
        (get-otp)
        (fetch)
        (save))))

(comment
  (def otp (get-otp "STK"))
  (fetch otp)
  (-main))
