(ns sector
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.string :as str]))

(defmacro redis [& body] `(r/wcar (env :redis-stock) ~@body))

(defn fetch
  [url]
  (log/debug "Downloading Data from " url)
  (let [res  (client/get url {:as :byte-array})
        body (:body res)]
    (log/info "Received in" (:request-time res) "ms")
    (String. body "euc-kr")))

(defn get-sectors
  []
  ; <a href="/sise/sise_group_detail.nhn?type=upjong&no=174">가정용품</a>
  (let [data  (fetch "http://finance.naver.com/sise/sise_group.nhn?type=upjong")
        regex (re-pattern "no=(.+)\">(.+)</a>")]
    (for [line (str/split-lines data)
          :when (str/includes? line "sise_group_detail.nhn")]
      (let [matches (re-find regex line)
            no      (read-string (get matches 1))
            name    (get matches 2)]
        (log/debug "found" name)
        (redis (r/hmset "sector" no name))
        no))))

(defn get-stocks
  [sector-no]
  ;<td><a href="/item/main.nhn?code=018250">애경산업</a> <span class="dot"></span></td>
  (log/debug "Parsing stocks of sector:" sector-no)
  (let [data  (fetch (str "http://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no=" sector-no))
        regex (re-pattern "code=(.+)\">.+</a>")]
    (doseq [line (str/split-lines data)
            :when (str/includes? line "main.nhn")]
      (let [matches (re-find regex line)
            code (get matches 1)]
        (redis (r/sadd (str "sector:" sector-no) (str "stock:" code))
               (r/hmset (str "stock:" code) "sector" (str "sector:" sector-no)))))))

(defn -main
  []
  (log/info "업종분류 (from NAVER)")
  (doseq [sector (get-sectors)]
    (get-stocks sector)))
