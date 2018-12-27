(ns stock-etf
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.string :as s]))

(def url "https://finance.naver.com/api/sise/etfItemList.nhn")

(def redis-uri (or (env :redis-uri) "redis://localhost:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(defn download
  [url]
  (let [options {:as             :byte-array
                 :socket-timeout 3000
                 :retry-handler  (fn [ex try-count http-context]
                                   (log/error ex)
                                   (if (> try-count 4) false true))}]
    (-> url
        (client/get options)
        :body
        (String. "euc-kr"))))

(defn parse
  [data]
  (for [row (-> (json/read-str data)
                (get "result")
                (get "etfItemList"))]
    {:symbol (get row "itemcode")
     :name   (get row "itemname")}))

(defn -main
  []
  (log/info "ETF (from NAVER)")
  (doseq [{:keys [symbol name]} (-> url
                                    download
                                    parse)]
    (log/debug symbol name)
    (redis (r/sadd "etf" (str "stock:" symbol))
           (r/hmset (str "stock:" symbol) "etf" 1))))

(comment
  (def -data (download url))
  (doseq [{:keys [symbol name]} (parse -data)]
    (redis (r/sadd "etf" (str "stock:" symbol))
           (r/hmset (str "stock:" symbol) "etf" 1)))

  (-main))

