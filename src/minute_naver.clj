(ns minute-naver
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [clj-http.client :as client]
            [clojure.core.async :as async :refer [>!! >! <! <!!]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as r]))

(defmacro redis [& body] `(r/wcar {:pool {} :spec {:uri (env :redis-uri)}} ~@body))

(def DATE (.format
            (java.text.SimpleDateFormat. "yyyy-MM-dd")
            (new java.util.Date)))
(def DIR (format "../data.minute/%s" DATE))
(def SYMBOLS
  (for [s (shuffle (redis (r/keys "stock:*")))
        :when (not (contains? (redis (r/smembers "etf")) s))]
    (str/replace s "stock:" "")))

(defn generate-url
  [symbol page]
  (let [d (.format
            (java.text.SimpleDateFormat. "yyyyMMddHHmmss")
            (new java.util.Date))]
    (format
      "http://finance.naver.com/item/sise_time.nhn?code=%s&page=%s&thistime=%s"
      symbol page d)))
(comment (def -url (generate-url "015760" 1)))

(defn download [url]
  (let [options {:as             :byte-array
                 :socket-timeout 3000
                 :retry-handler  (fn [ex try-count http-context]
                                   (log/error "Error:" ex)
                                   (if (> try-count 4) false true))}]
    (-> url
        (client/get options)
        :body
        (String. "euc-kr"))))
(comment (def -html (download -url)))

(defn parse-last-page [lines]
  (try
    (->> lines
         (filter #(str/includes? % "맨뒤"))
         first
         (re-find #"page=(\d+)")
         last
         read-string)
    (catch NullPointerException e
      1)))
(comment (parse-last-page (str/split-lines -html)))

(defn parse-rows [lines]
  (transduce
    (comp
      (filter
        #(or (str/includes? % "<td class=\"num\"><span class=\"tah p11\">")
             (str/includes? % "<td align=\"center\"><span class=\"tah p10 gray03\">")))
      (map #(str/replace % "," ""))
      (map #(re-find #"<span class=\".+\">(.+)</span>" %))
      (map last)
      (filter some?)
      (partition-all 6))
    conj
    lines))
(comment (def -rows (parse-rows (str/split-lines -html))))

(defn save
  [symbol rows]
  (with-open [w (io/writer (format "%s/%s.txt" DIR symbol) :append true)]
    (binding [*out* w]
      (doseq [[hhmm price _ _ _ volume] rows]
        (println
          (format
            "minute,symbol=%s price=%si,volume=%si %s"
            symbol
            price
            volume
            (-> (format "%sT%s:00Z" DATE hhmm)
                (java.time.Instant/parse)
                (.getEpochSecond)))))))
  (log/trace (str "Saved: " symbol " " (count rows))))
(comment (save "015760" -rows))

(defn process
  [symbol]
  (let [lines (-> (generate-url symbol 1)
                  download
                  str/split-lines)
        last-page (parse-last-page lines)]
    (loop [total-rows 0
           page 1
           rows (parse-rows lines)]
      (if (>= last-page page)
        (do
          (when (seq rows)
            (save symbol rows))
          (recur
            (+ total-rows (count rows))
            (inc page)
            (-> (generate-url symbol (inc page))
                download
                str/split-lines
                parse-rows)))
        (do
          (log/debug (format "[%s] %,d rows saved" symbol total-rows))
          {:symbol symbol :rows total-rows})))))
(comment (process "015760"))
(comment (process "20058AAA0"))

(defn -main []
  (io/make-parents (format "%s/file" DIR))

  (time (<!!
          (async/pipeline-blocking
            (* 2 (.availableProcessors (Runtime/getRuntime)))
            (doto (async/chan) (async/close!))
            (map process)
            (async/to-chan SYMBOLS)
            true
            (fn [err] (log/error (.getMessage err))))))

  (log/debug "Done")
  )

(comment
  (-main)

  (comment (def symbol (first symbols)))

  (def SYMBOLS ["015760" "047040"])
  (async/go (async/>! output {:symbol "015760" :rows 420}))
  (async/go (println (async/<! output)))


  (.getEpochSecond (java.time.Instant/parse "2018-04-18T12:34:56Z"))
  (-> (str date "T" time "Z")
      (java.time.Instant/parse)
      (.toEpochMilli)
      ;      (- 32400000)                                          ; +09:00 to UTC
      (+ millisec))
  )

