(ns yquant.stock.price.current
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.string :as str]))

(def target "현재가격 (DAUM)")

(defn fetch [url]
  (log/info "Requesting Data:" target url)
  (let [res (client/get url)]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(def -conn (env :redis-stock))
(defmacro redis [& body] `(r/wcar -conn ~@body))

(defn set-timestamp [key]
  (redis (r/set key
                (quot (System/currentTimeMillis) 1000))))

(def regex (re-pattern "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn"))

(defn save [data]
  (log/info "Saving to" -conn)
  (doseq [line (str/split-lines data)
          :let [matches (re-find regex line)]
          :when (str/includes? line "code")]
    (let [code  (get matches 1)
          name  (get matches 2)
          price (read-string (str/replace (get matches 3) "," ""))]
      (log/debug code name price)
      (redis (r/hmset (str "stock:" code)
                      "price" price))))
  (set-timestamp "price-timestamp"))

(defn main []
  (map #(-> %
            (fetch)
            (save))
       ["http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
        "http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S"]))

