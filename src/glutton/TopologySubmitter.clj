(ns glutton.TopologySubmitter
  (:require [glutton.topology :refer [stormy-topology]]
            [backtype.storm [config :refer :all]])
  (:import [backtype.storm StormSubmitter]))

(defn -main [& {debug "debug" workers "workers" :or {debug "false" workers "4"}}]
  (StormSubmitter/submitTopology
   "glutton topology"
   {TOPOLOGY-DEBUG (Boolean/parseBoolean debug)
    TOPOLOGY-WORKERS (Integer/parseInt workers)}
   (stormy-topology)))
