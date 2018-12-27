(ns price-current
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.string :as str]))

(def redis-uri (or (env :redis-uri) "redis://localhost:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(def regex (re-pattern "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn"))


(defn fetch [url]
  (log/info "Requesting Data:" url)
  (let [res (client/get url)]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))


(defn save [data]
  (log/info "Saving to Redis")
  (doseq [line (str/split-lines data)
          :let [matches (re-find regex line)]
          :when (str/includes? line "code")]
    (let [symbol (get matches 1)
          name   (get matches 2)
          price  (read-string (str/replace (get matches 3) "," ""))]
      (log/debug symbol name price)
      (redis (r/hmset (str "stock:" symbol)
                      "name" name
                      "price" price
                      "updated" (quot (System/currentTimeMillis) 1000))))))

(defn -main []
  (log/info "현재가 (from DAUM)")
  (doseq [url ["http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
               "http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S"]]
    (-> url
        (fetch)
        (save))))

(comment
  (-main))

