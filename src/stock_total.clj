(ns stock-total
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
    (log/debug url)
    (-> url
        (client/get options)
        :body
        (String. "euc-kr"))))

(defn parse
  [lines]
  (transduce
    (comp
      (filter #(or (re-find #"<td class=\"number\">(.+)</td>" %)
                   (s/includes? % "/item/main.nhn?code=")))
      (map #(or (re-find #"<td class=\"number\">(.+)</td>" %)
                (re-find #"/item/main.nhn\?code=([0-9A-Z]+)\"" %)))
      (map last)
      (map #(s/replace % "," ""))
      (map #(s/replace % "\t" ""))
      (map #(s/replace % "N/A" "-1"))
      ;(filter some?)
      (partition-all 9)
      )
    conj
    lines))

(defn convert
  [rows]
  (for [row rows]
    (merge {:symbol (first row)}
           (->> (rest row)
                (map read-string)
                (zipmap [:price
                         :unit-price
                         :total-value
                         :shares
                         :foreigner
                         :volume
                         :PER
                         :ROE])))))

(defn save
  [rows]

  )


(defn parse-last-page
  [lines]
  (try
    (->> lines
         (filter #(s/includes? % "맨뒤"))
         first
         (re-find #"page=(\d+)")
         last
         read-string)
    (catch NullPointerException e 1)))

(defn save
  [rows]
  (log/debug rows)
  rows)

(defn process
  [url]
  (let [url       (format url 1)
        html      (download url)
        lines     (s/split-lines html)
        last-page 1]
    ;last-page (parse-last-page lines)]
    (loop [total-rows 0
           page       1
           rows       (-> lines
                          parse
                          convert)]
      (if (<= page last-page)
        (do
          (when (seq rows)
            (save rows))
          (recur
            (+ total-rows (count rows))
            (inc page)
            (-> (format url (inc page))
                download
                s/split-lines
                parse
                convert)))
        (do
          (log/info (format "[%s] %,d rows saved" symbol total-rows))
          {:rows total-rows})))))

(defn -main
  []
  (log/info "Total information (from NAVER)")
  )

(comment
  (def url "https://finance.naver.com/sise/sise_market_sum.nhn?sosok=0&page=1")
  (def html (download url))
  (def lines (s/split-lines html))
  (def rows (parse lines))
  (convert rows)


  (-> url
      download
      s/split-lines
      parse
      convert)

  (process
    "https://finance.naver.com/sise/sise_market_sum.nhn?sosok=0&page=%s")


  (-main))

