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
  (if-let [res (try (client/post url options)
                    (catch Exception e
                      (log/error "Requesting Timeout")))]
    (if (= 200 (:status res))
      res
      (log/error (format "Response Error (Status:%s)" (:status res))))))

(defn fetch
  [symbol]
  (log/debug "Requesting" symbol)
  (let [fullcode (redis (r/hget symbol "fullcode"))
        otp (client/get "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD/08/0801/08010700/mkd08010700_02&name=form")
        params {:code      (:body otp)
                :isu_cd    fullcode
                :acsString "1"}
        res (post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                  {:form-params    params
                   :socket-timeout 60000
                   :conn-timeout   3000})]
    (when res (log/trace "Received" (:length res) "bytes in" (:request-time res) "ms"))
    (:body res)))

(defn parse [data]
  (for [row (:block1 (json/read-json data))
        :let [time (:hms row)
              price (:isu_cur_pr row)
              volume (:isu_tr_cnt row)]]
    {:time time :price price :volume volume}))

(defn save [symbol rows]
  (with-open [w (io/writer (get-filepath symbol) :append true)]
    (binding [*out* w]
      (doseq [row rows
              :let [time (:time row)
                    symbol-short (str/replace symbol "stock:" "")
                    price (str/replace (:price row) "," "")
                    volume (str/replace (:volume row) "," "")]]
        (println time symbol-short price volume))))
  (when (seq rows) (log/debug (format "Saved %,d records of %s" (count rows) symbol)))
  rows)

(defn run [symbol]
  (io/make-parents (get-filepath symbol))
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
  (fetch "stock:140950")
  (run "stock:140950"))

