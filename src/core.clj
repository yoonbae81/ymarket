(ns core
  (:require [clojure.java.io :as io]
            [clj-http.client :as client]
            [taoensso.timbre :as log]
            [stock-symbol :as symbol]
            [stock-etf :as etf]
            [stock-sector :as sector]
            [price-day :as day]
            [price-minute :as minute]))

(def telegram-config (-> "telegram.edn"
                         io/resource
                         slurp
                         read-string))

(defn telegram
  [message]
  (let [{:keys [url token chat-id]} telegram-config
        url (format url token)]
    (client/post url
                 {:async?      true
                  :form-params {:chat_id chat-id
                                :text    message}}
                 (fn [res])
                 (fn [err] (log/error (.getMessage err)))))
  nil)

(defn -main
  []
  (symbol/-main)
  (etf/-main)
  (sector/-main)
  (day/-main)
  (minute/-main)
  (telegram "StockData Done"))

(comment
  (-main))
