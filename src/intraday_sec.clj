(ns intraday-sec
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log])
  (:import (java.util.concurrent Executors)))

(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri (env :redis-uri)}} ~@body))

(def ETFs (set (redis (r/smembers "etf"))))
(def date (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date)))

(defn get-filepath [symbol]
  (format "../data.intraday/%s/%s.txt"
          date
          (str/replace symbol "stock:" "")))

(defn post [url options]
  (loop [retry 4]
    (if-let [res (try (client/post url options)
                      (catch Exception e
                        (log/error "Requesting Timeout")))]
      (if (= 200 (:status res))
        res
        (log/error (format "Response Error (Status:%s)" (:status res))))
      (recur (dec retry)))))

(defn fetch
  ([symbol] (fetch symbol 1))
  ([symbol page]
   (log/debug (format "Requesting page %,d of %s" page symbol))
   (let [fullcode (redis (r/hget symbol "fullcode"))
         otp (client/get "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD/04/0402/04020100/mkd04020100t3_01&name=form")
         params {:code    (:body otp)
                 :isu_cd  fullcode
                 :curPage page}
         res (client/post "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
                          {:form-params    params
                           :socket-timeout 180000
                           :conn-timeout   3000})]
     (when res (log/trace "Received" (:length res) "bytes in" (:request-time res) "ms"))
     (:body res))))

(defn parse [data]
  (for [row (:result (json/read-json data))
        :let [time (:trd_ddtm row)
              price (:trd_prc row)
              volume (:acc_trdvol row)]]
    {:time time :price price :volume volume}))

(defn save [symbol rows]
  (with-open [w (io/writer (get-filepath symbol) :append true)]
    (binding [*out* w]
      (doseq [row rows
              :let [time (:time row)
                    symbol-short (str/replace symbol "stock:" "")
                    price (str/replace (:price row) "," "")
                    volume (:volume row)]]
        (println time symbol-short price volume))))
  (when (seq rows) (log/trace (format "Saved %,d records of %s" (count rows) symbol)))
  rows)

(defn run [symbol]
  (if (.exists (io/as-file (get-filepath symbol)))
    (log/info "File exists" symbol "(skip)")
    (do
      (io/make-parents (get-filepath symbol))
      (loop [acc 0
             page 1]
        (let [rows (->> (fetch symbol page)
                        (parse)
                        (save symbol))]
          (if (empty? rows)
            acc
            (recur (+ acc (count rows)) (inc page))))))))

(defn -main []
  (let [count (atom 0)
        ;pool    (Executors/newFixedThreadPool (+ 4 (.availableProcessors (Runtime/getRuntime))))
        pool (Executors/newFixedThreadPool 20)
        symbols (for [symbol (shuffle (redis (r/keys "stock:*")))
                      :when (not (contains? ETFs symbol))]
                  symbol)
        tasks (map (fn [symbol]
                     (fn [] (swap! count + (run symbol))))
                   symbols)]
    (doseq [future (.invokeAll pool tasks)]
      (.get future))
    (.shutdown pool)
    (log/info (format "Total Count: %,d" @count))))

(comment
  (fetch "stock:015760"))
