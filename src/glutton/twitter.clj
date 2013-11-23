(ns glutton.twitter
  (:use [twitter.oauth]
        [twitter.callbacks]
        [twitter.callbacks.handlers]
        [twitter.api.streaming])
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import (twitter.callbacks.protocols AsyncStreamingCallback)))

;; eval block to begin callback stream
(let [callback (AsyncStreamingCallback.
                (fn [_resp payload]
                  (let [tweet (-> (str payload) json/read-json)]
                    (spit "twitter-sample" (str tweet "\n") :append true)))
                (fn [_resp]
                  (println "closing connection..."))
                (fn [_resp ex]
                  (.printStackTrace ex)))]
  (def sample
    (statuses-sample :oauth-creds my-creds
                     :callbacks callback)))

;; eval to stop stream
    ((:cancel (meta sample)))