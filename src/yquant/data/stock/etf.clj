(ns yquant.data.stock.etf
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(def target "종목코드-ETF (KRX)")

(defn get-otp []
  (log/info "Requesting OTP:" target)
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name "selectbox"
                             :bld  "COM/hpetpcm03"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/info "Requesting Data:" target)
  (let [res
        (client/post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                     {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(def -conn (env :redis-stock))
(defmacro redis [& body] `(r/wcar -conn ~@body))

(defn save [data]
  (log/info "Saving to" -conn)
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

(defn main []
  (-> (get-otp)
      (fetch)
      (save)))

