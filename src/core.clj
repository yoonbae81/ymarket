(ns core
  (:require [yquant.data.stock.etf :as etf]
            [yquant.data.stock.securities :as securities]
            [yquant.data.stock.sector :as sector]
            [yquant.data.stock.price :as price]
            [yquant.data.stock.value :as value]
            [yquant.data.stock.prohibited :as prohibited]))

(defn main []
  (etf/main)
  (securities/main)
  (sector/main)
  (price/main)
  (value/main)
  (prohibited/main)
  )

(comment
  (main))
