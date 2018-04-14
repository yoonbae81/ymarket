(ns yquant.data.stock.prohibited
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(def target "거래정지/관리종목 (KRX)")

(defn get-otp [url]
  (log/info "Requesting OTP:" target)
  (let [res (client/post
              "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
              {:form-params {:name     "fileDown"
                             :filetype "csv"
                             :url      url
                             :gubun    "ALL"}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn fetch [otp]
  (log/info "Requesting Data:" target)
  (let [res
        (client/post "http://file.krx.co.kr/download.jspx"
                     {:form-params {:code otp}})]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(def -conn (env :redis-stock))
(defmacro redis [& body] `(r/wcar -conn ~@body))

(defn save [data]
  (log/info "Saving to" -conn)
  (doseq [line (rest (str/split-lines data))
          :let [comma (str/index-of line, ",")
                code  (subs line 0 comma)]]
    (log/debug code)
    (redis (r/sadd "prohibited" (str "stock:" code)))))

(def urls
  {:관리종목     "MKD/04/0403/04030100/mkd04030100"
   :정리매매종목   "MKD/04/0403/04030200/mkd04030200"
   :매매거래정지종목 "MKD/04/0403/04030300/mkd04030300"})

(defn main []
  (map #(-> %
            (get-otp)
            (fetch)
            (save))
       (vals urls)))

(comment
  (def url "MKD/04/0403/04030300/mkd04030300"))
