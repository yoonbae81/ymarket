(ns intraday2
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(def ETFs (set (redis (r/smembers "etf"))))
(def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))
(def line-protocol "intraday,symbol=%s price=%s,volume=%s %s")

(def counter (atom 1000))
(defn next-value
  []
  (when (zero? @counter) (reset! counter 1000))
  (swap! counter dec))

(defn get-timestamp [date time millisec]
  ; (t.EpochMilli (java.time.Instant/parse "2018-04-18T12:34:56Z"))
  (-> (str date "T" time "Z")
      (java.time.Instant/parse)
      (.toEpochMilli)
      ;      (- 32400000)                                          ; +09:00 to UTC
      (+ millisec)))

(defn post [url options]
  (let [opts (merge options
                    {:socket-timeout 90000
                     :conn-timeout   3000})]
    (loop [retry 2]
      (if-let [res (try (client/post url opts)
                        (catch Exception e
                          (log/error "Failed")
                          (if (zero? retry)
                            {:body "" :length 0 :request-timnde 9999}
                            nil)))]
        res
        (recur (dec retry))))))

(defn get-otp [url]
  (log/trace "Requesting OTP")
  (let [res (client/get url)]
    (log/trace "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))
(comment {:form-params {:name      "fileDown"
                        :filetype  "csv"
                        :url       url
                        :isu_cd    fullcode
                        :aceString "1"}})
(defn fetch [otp fullcode page]
  (let [res    (post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                     {:form-params {:code   otp
                                    :isu_cd fullcode
                                    :curPage page
                                    }})
        length (:length res)
        body   (:body res)]
    (log/trace "Received" (format "%,d" length) "bytes in" (:request-time res) "ms")
    body
    ))
(comment
  (-> (get-otp "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD/04/0402/04020100/mkd04020100t3_01&name=chart")
      (fetch "KR7005930003" 4900)
      (json/read-json))
  )

;(if (str/ends-with? body "\"")
;  body
;  (str body "00\""))

(defn parse [data idx]
  (for [row (rest (csv/read-csv data))
        :let [time   (get row (:time idx))
              price  (str/replace (get row (:price idx)) "," "")
              volume (str/replace (get row (:volume idx)) "," "")]
        :when (not (str/blank? volume))]
    {:time time :price price :volume volume}))

(comment
  (def line (first (rest (str/split-lines data))))
  (def line (last (str/split-lines data))))

(defn parse [data idx]
  (doseq [line (rest (str/split-lines data))
          :let [row (try (csv/read-csv line)
                         (catch Exception e nil))]
          :when (not (nil? row))]
    (let [time   (get row (:time idx))
          price  (str/replace (get row (:price idx)) "," "")
          volume (str/replace (get row (:volume idx)) "," "")]
      (println {:time time :price price :volume volume}))))

(comment
  (parse data (if (contains? ETFs symbol)
                {:time 0 :price 1 :volume 6}
                {:time 1 :price 2 :volume 4})))

(defn save [rows symbol]
  (let [filepath (str "../data.intraday/" date "/" symbol ".txt")]
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
          (println (format line-protocol
                           symbol price volume timestamp)))))))

(defn run [symbol]
  (log/info "Downloading" symbol (redis (r/hget symbol "name")))
  (let [url      (if (contains? ETFs symbol)
                   "MKD/08/0801/08010700/mkd08010700_02"
                   "MKD/04/0402/04020100/mkd04020100t3_01")
        fullcode (redis (r/hget symbol "fullcode"))
        idx      (if (contains? ETFs symbol)
                   {:time 0 :price 1 :volume 6}
                   {:time 1 :price 2 :volume 4})]
    (-> (get-otp url fullcode)
        (fetch)
        (parse idx)
        (save (str/replace symbol "stock:" "")))))

(comment
  (def symbol "stock:015760")
  (def symbol "stock:005930")
  (def symbol "stock:112240")
  (def symbol "stock:069500")                               ; KODEX200                              ;
  ; KODEX KTOP30 makes EOFException
  (def symbol "stock:229720")
  (run symbol)
  (def data
    (-> (get-otp (if (contains? ETFs symbol)
                   "MKD/08/0801/08010700/mkd08010700_02"
                   "MKD/04/0402/04020100/mkd04020100t3_01")
                 (redis (r/hget symbol "fullcode")))
        (fetch)))


  (doseq [line (str/split-lines data)
          :let [row (csv/read-csv line)]]
    (try
      (println row)
      (catch Exception e
        (println "Oops")
        )))

  )

(defn -main []
  (log/info "Intraday Prices (from KRX)")
  (doseq [symbol (redis (r/keys "stock:*"))
          :let [stock (redis (r/hgetall* symbol))]]
    (run symbol)))

