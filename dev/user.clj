(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.test :as test]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [glutton.topology :refer [stormy-topology]]
            [glutton.bolts :refer [stormy-bolt glutton-bolt]]
            [glutton.spouts :refer [type-spout]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster LocalDRPC]))

(def cluster (atom nil))

(defn stop
  []
  (if-let [lc @cluster]
    (do (.shutdown lc)
        (reset! cluster nil))))

(defn start
  [& {debug "debug" workers "workers" :or {debug "true" workers "2"}}]
  (let [lc (LocalCluster.)]
    (when @cluster (stop))
    (.submitTopology lc "stormy topology"
                     {TOPOLOGY-DEBUG (Boolean/parseBoolean debug)
                      TOPOLOGY-WORKERS (Integer/parseInt workers)}
                     (stormy-topology))
    (reset! cluster lc)))
  
(defn restart
  []
  (do
    (stop)
    (refresh)
    (start)))


