(ns intraday-minutes
  (:require [environ.core :refer [env]]
            [clojure.core.async :as async :refer [>!! <!!]]))

(let [c (async/chan 10)]
  (>!! c "hello")  ;; => 대기(blocking)
  (assert (= "hello" (<!! c)))
  (async/close! c))


(def symbols ["015760" "047040"])