(ns core
  (:require [clojure.java.io :as io]
            [stock-symbol :as symbol]
            [stock-etf :as etf]
            [stock-sector :as sector]
            [price-day :as day]
            [price-minute :as minute]))





(defn -main
  []
  (symbol/-main)
  (etf/-main))


(comment


  (-main) )
