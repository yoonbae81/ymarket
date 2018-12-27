(ns stock-symbol
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.string :as s]))

(def redis-uri (or (env :redis-uri) "redis://localhost:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(def regex (re-pattern "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn"))

(defn download
  [url]
  (log/info "Requesting Data:" url)
  (let [res (client/get url)]
    (log/info "Received" (:length res) "bytes in" (:request-time res) "ms")
    (:body res)))

(defn parse
  [html]
  (let [regex (re-pattern "code:\"(.+)\",name :\"(.+)\",cost :\"(.+)\",updn")]
    (for [line (s/split-lines html)
          :let [matches (re-find regex line)]
          :when (s/includes? line "code")]
      (let [symbol (get matches 1)
            name   (get matches 2)
            price  (read-string (s/replace (get matches 3) "," ""))]
        {:symbol symbol :name name :price price}))))

(defn save
  [rows]
  (log/info "Saving to Redis")
  (doseq [line (s/split-lines rows)
          :let [matches (re-find regex line)]
          :when (s/includes? line "code")]
    (let [symbol (get matches 1)
          name   (get matches 2)
          price  (read-string (s/replace (get matches 3) "," ""))]
      (log/debug symbol name price)
      (redis (r/hmset (str "stock:" symbol)
                      "name" name
                      "price" price
                      "updated" (quot (System/currentTimeMillis) 1000))))))

(defn -main []
  (log/info "종목코드 (from DAUM)")
  (doseq [url ["http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S"
               "http://finance.daum.net/xml/xmlallpanel.daum?stype=Q&type=S"]]
    (doseq [{:keys [symbol name price]} (-> url
                                            download
                                            parse)]
      (log/debug symbol price name)
      (redis (r/hmset (str "stock:" symbol)
                      "name" name
                      "price" price
                      "updated" (quot (System/currentTimeMillis) 1000))))))

(comment
  (def -url "http://finance.daum.net/xml/xmlallpanel.daum?stype=P&type=S")
  (def -html (download -url))
  (parse -html)

  (-main))

