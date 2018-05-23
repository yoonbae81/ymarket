(ns intraday
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(def ETFs (set (redis (r/smembers "etf"))))
(def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))
(def basedir (str "../data.intraday/" date))

(defn post [url options]
  (if-let [res (try (client/post url options)
                    (catch Exception e
                      (log/error "Response Timeout")))]
    (if (= 200 (:status res))
      res
      (log/error (format "Response Error (Status:%s)" (:status res))))))

(defn fetch [symbol]
  (let [fullcode (redis (r/hget symbol "fullcode"))
        url (if (contains? ETFs symbol)
              "MKD/08/0801/08010700/mkd08010700_02"
              "MKD/04/0402/04020100/mkd04020100t3_01")
        params {:name      "fileDown"
                :filetype  "csv"
                :isu_cd    fullcode
                :url       url
                :aceString "1"}
        otp (post "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
                  {:form-params params})
        res (post "http://file.krx.co.kr/download.jspx"
                  {:form-params    {:code (:body otp)}
                   :socket-timeout 60000
                   :conn-timeout   1000})]
    (when res (log/debug "Received" (:length res) "bytes in" (:request-time res) "ms"))
    (:body res)))

(defn parse [symbol data]
  (when (not (str/blank? data))
    (let [idx (if (contains? ETFs symbol)
                {:count 7 :time 0 :price 1 :volume 6}
                {:count 6 :time 1 :price 2 :volume 4})]
      (for [line (rest (str/split-lines data))
            :let [row (-> line
                          (str/replace #"(\d),(\d)" "$1$2")
                          (str/replace #"\"" "")
                          (str/split #","))]
            :when (= (count row) (:count idx))]
        (let [time (get row (:time idx))
              price (get row (:price idx))
              volume (get row (:volume idx))]
          {:time time :price price :volume volume})))))

(defn save [symbol filepath rows]
  (when rows
    (io/make-parents filepath)
    (with-open [w (io/writer filepath)]
      (binding [*out* w]
        (doseq [row rows
                :let [time (:time row)
                      symbol-short (str/replace symbol "stock:" "")
                      price (:price row)
                      volume (:volume row)]]
          (println time symbol-short price volume))))
    (count rows)))

(defn run [symbol]
  (let [symbol-short (str/replace symbol "stock:" "")
        symbol-name (redis (r/hget symbol "name"))
        filepath (format "%s/%s.txt" basedir symbol-short)]
    (if (.exists (io/as-file filepath))
      (log/info "File exists (skip)" symbol-name symbol-short)
      (do
        (log/info "Downloading" symbol-name symbol-short)
        (->> (fetch symbol)
             (parse symbol)
             (save symbol filepath))))))

(defn -main []
  (doseq [symbol (shuffle (redis (r/keys "stock:*")))
          :let [stock (redis (r/hgetall* symbol))]]
    (run symbol)))

(comment
  (def symbol "stock:015760")
  (def symbol "stock:005930")
  (def symbol "stock:112240")
  (def symbol "stock:069500")                               ; KODEX200                              ;
  ; KODEX KTOP30 makes EOFException
  (def symbol "stock:229720")
  ; KODEX 레버리지 never succeed
  (def symbol "stock:122630")
  (redis (r/hgetall* symbol))
  (def symbol "stock:222080")
  (def data (fetch symbol))
  (def line (first (rest (str/split-lines data))))
  (def line (last (str/split-lines data)))
  (def rows (parse symbol data))
  (save symbol "~/Desktop/test.txt" rows)
  (run symbol)
  )
