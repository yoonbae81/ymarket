(ns intraday-minutes
  (:require [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.core.async :as async :refer [>!! <!!]]))
(.format (java.text.SimpleDateFormat. "yyyyMMddHHmmss") (new java.util.Date))
(def URL "http://finance.naver.com/item/")

(let [c (async/chan 10)]
  (>!! c "hello")                                           ;; => 대기(blocking)
  (assert (= "hello" (<!! c)))
  (async/close! c))

(def symbols ["015760" "047040"])

(def url "http://finance.naver.com/item/sise_time.nhn?code=007390&thistime=20180614210000&page=1")

(-> url
    (client/get {:as :byte-array})
    :body
    (String. "euc-kr"))

(defn generate-url
  ([symbol] (generate-url symbol 1))
  ([symbol page]
   (format
     "http://finance.naver.com/item/sise_time.nhn?code=%s&thistime=%s&page=%s"
     symbol
     (.format (java.text.SimpleDateFormat. "yyyyMMddHHmmss") (new java.util.Date))
     page)))
(comment (generate-url "015760"))

(defn download [url]
  (-> url
      (client/get {:as :byte-array})
      :body
      (String. "euc-kr")))

(transduce
  (comp
    (map generate-url)
    (map download))
  conj
  symbols)
