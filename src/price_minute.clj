(ns price-minute
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.core.async :as a :refer [>!! >! <! <!!]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as r]))

(def date (java.time.LocalDate/now))
(def basedir "../data.minute")

(def redis-uri (or (env :redis-uri) "redis://192.168.0.3:6379"))
(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri redis-uri}} ~@body))

(defn get-symbols []
  (for [symbol (shuffle (redis (r/keys "stock:*")))]
    (s/replace symbol "stock:" "")))

(defn get-url
  [symbol page]
  (let [d (.format date (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd"))]
    (format "http://finance.naver.com/item/sise_time.nhn?code=%s&page=%s&thistime=%s180000"
            symbol page d)))

(defn download [url]
  (let [options {:as             :byte-array
                 :socket-timeout 3000
                 :retry-handler  (fn [ex try-count http-context]
                                   (log/error ex)
                                   (if (> try-count 4) false true))}]
    (-> url
        (client/get options)
        :body
        (String. "euc-kr"))))

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

(defn parse
  [lines]
  (transduce
    (comp
      (filter
        #(or (s/includes? % "<td class=\"num\"><span class=\"tah p11\">")
             (s/includes? % "<td align=\"center\"><span class=\"tah p10 gray03\">")))
      (map #(s/replace % "," ""))
      (map #(re-find #"<span class=\".+\">(.+)</span>" %))
      (map last)
      (filter some?)
      (partition-all 6))
    conj
    lines))

(defn save
  [symbol rows]
  (let [dir  (->> "yyyy-MM-dd"
                  java.time.format.DateTimeFormatter/ofPattern
                  (.format date))
        file (format "%s/%s/%s.txt" basedir dir symbol)
        _    (io/make-parents file)]
    (with-open [w (io/writer file :append true)]
      (binding [*out* w]
        (doseq [[hhmm price _ _ _ volume] rows
                :let [timestamp (-> (format "%sT%s:00Z" date hhmm)
                                    (java.time.Instant/parse)
                                    (.getEpochSecond))]]
          (println (format "%s %s %s %s"
                           timestamp
                           symbol
                           price
                           volume))))))
  (log/trace (str "Saved: " symbol " " (count rows))))

(defn process
  [symbol]
  (let [url       (get-url symbol 1)
        html      (download url)
        lines     (s/split-lines html)
        last-page (parse-last-page lines)]
    (loop [total-rows 0
           page       1
           rows       (parse lines)]
      (if (<= page last-page)
        (do
          (when (seq rows)
            (save symbol rows))
          (recur
            (+ total-rows (count rows))
            (inc page)
            (-> (get-url symbol (inc page))
                download
                s/split-lines
                parse)))
        (do
          (log/info (format "[%s] %,d rows saved" symbol total-rows))
          {:symbol symbol :rows total-rows})))))

(defn -main []
  (time
    (<!!
      (a/pipeline-blocking
        (* 2 (.availableProcessors (Runtime/getRuntime)))
        (doto (a/chan) (a/close!))
        (map process)
        (a/to-chan (get-symbols))
        true
        (fn [err] (log/error (.getMessage err)))))))

(comment
  (-main)

  (def date (java.time.LocalDate/now))
  (def date (.minusDays date 1))

  (parse-last-page (s/split-lines -html))

  (def -symbol "131290")
  (def -url (get-url -symbol 1))
  (def -html (download -url))
  (def -lines (s/split-lines -html))
  (def -rows (parse -lines))
  (def -lps (convert -symbol -rows))
  (save -lps)

  (redis (r/ping))

  (a/go (a/>! output {:symbol "015760" :rows 420}))
  (a/go (println (a/<! output)))
  )

