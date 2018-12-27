(ns stock-sector
  (:require [environ.core :refer [env]]
            [taoensso.carmine :as r]
            [taoensso.timbre :as log]
            [clj-http.client :as client]
            [clojure.string :as s]))

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

(defn get-sectors []
  ; <a href="/sise/sise_group_detail.nhn?type=upjong&no=174">가정용품</a>
  (let [html  (download "http://finance.naver.com/sise/sise_group.nhn?type=upjong")
        regex (re-pattern "no=(.+)\">(.+)</a>")]
    (for [line (s/split-lines html)
          :when (s/includes? line "sise_group_detail")]
      (let [matches (re-find regex line)
            no      (read-string (get matches 1))
            name    (get matches 2)]
        (log/debug "Section:" no name)
        {:no no :name name}))))

(defn get-symbols
  [sector-no sector-name]
  ;<td><a href="/item/main.nhn?code=018250">애경산업</a> <span class="dot"></span></td>
  (let [html  (download (str "http://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no=" sector-no))
        regex (re-pattern "code=(.+)\">.+</a>")]
    (for [line (s/split-lines html)
          :when (s/includes? line "main.nhn")]
      (let [matches (re-find regex line)
            symbol  (get matches 1)]
        (log/debug "Section:" sector-no sector-name symbol)
        {:symbol symbol :sector sector-name}))))

(defn -main
  []
  (log/info "업종분류 (from NAVER)")
  (log/info "Downloading sector list")
  (doseq [{:keys [no name]} (get-sectors)]
    (doseq [{:keys [symbol sector]} (get-symbols no name)]
      (redis (r/sadd (str "sector:" sector) (str "stock:" symbol))
             (r/hmset (str "stock:" symbol) "sector" sector)))))

(comment
  (-main))
