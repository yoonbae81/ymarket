(ns yquant.stock.symbol.prohibited
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn get-otp [url]
  (log/info "Requesting OTP")
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name     "fileDown"
                             :filetype "csv"
                             :url      url
                             :gubun    "ALL"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/debug "Downloading Data")
  (let [res
        (client/post "http://file.krx.co.kr/download.jspx"
                     {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn save [data]
  (log/info "Saving to Redis")
  (doseq [line (rest (str/split-lines data))
          :let [comma (str/index-of line, ",")
                code  (subs line 0 comma)]]
    (log/debug code)
    (redis (r/sadd "prohibited" (str "stock:" code)))))

(def urls
  {"관리종목"     "MKD/04/0403/04030100/mkd04030100"
   "정리매매종목"   "MKD/04/0403/04030200/mkd04030200"
   "매매거래정지종목" "MKD/04/0403/04030300/mkd04030300"})

(defn -main []
  (doseq [[name, url] urls]
    (log/info name "(from KRX)")
    (-> url
        (get-otp)
        (fetch)
        (save))))

