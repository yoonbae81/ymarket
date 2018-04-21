(ns securities
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clojure.string :as str]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn get-otp []
  (log/info "Requesting OTP")
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name "form"
                             :bld  "COM/finder_stkisu"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/debug "Downloading Data")
  (let [res
        (client/post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                     {:form-params {:code   otp
                                    :mktsel "ALL"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn save [data]
  (log/info "Saving to Redis")
  (doseq [row (get (json/read-str data) "block1")]
    (let [symbol     (subs (get row "short_code") 1)          ; delete preceding A
          name     (get row "codeName")
          market   (str/lower-case (get row "marketName"))
          fullcode (get row "full_code")]
      (log/debug symbol name)
      (redis (r/hmset (str "stock:" symbol)
                      "name" name
                      "market" market
                      "fullcode" fullcode)
             (r/sadd market (str "stock:" symbol))))))

(defn -main []
  (log/info "유가증권 종목코드 (from KRX)")
  (-> (get-otp)
      (fetch)
      (save)))

