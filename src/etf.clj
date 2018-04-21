(ns etf
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn get-otp []
  (log/info "Requesting OTP")
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name "selectbox"
                             :bld  "COM/hpetpcm03"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/debug "Downloading Data")
  (let [res
        (client/post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                     {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn save [data]
  (log/info "Saving to Redis")
  (doseq [row (get (json/read-str data) "block1")]
    (let [symbol   (subs (get row "value") 3 9)
          name     (get row "label")
          market   "kospi"
          fullcode (get row "value")]
      (log/debug symbol name)
      (redis (r/hmset (str "stock:" symbol)
                      "name" name
                      "market" market
                      "fullcode" fullcode)
             (r/sadd market (str "stock:" symbol))
             (r/sadd "etf" (str "stock:" symbol))))))

(defn -main []
  (log/info "ETF 종목코드 (from KRX)")
  (-> (get-otp)
      (fetch)
      (save)))

