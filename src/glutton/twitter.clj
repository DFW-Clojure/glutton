(ns glutton.twitter
  (:use [twitter.oauth]
        [twitter.callbacks]
        [twitter.callbacks.handlers]
        [twitter.api.streaming])
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [backtype.storm.clojure :refer [defspout spout emit-spout!]])
  (:import [twitter.callbacks.protocols AsyncStreamingCallback]
           [java.util.concurrent ConcurrentLinkedQueue]))

(defn- get-env! [name]
  (or (get (System/getenv) name)
      (throw (Exception. (format "'%s' is not set." name)))))

(def my-creds
  (apply make-oauth-creds (map get-env! ["TWITTER_APP_CONSUMER_KEY"
                                         "TWITTER_APP_CONSUMER_SECRET"
                                         "TWITTER_USER_ACCESS_TOKEN"
                                         "TWITTER_USER_ACCESS_TOKEN_SECRET"])))

(defn make-callback [queue]
  (AsyncStreamingCallback.
   (fn [_resp payload]
     (when-let [tweet (-> (str payload) (json/read-str :key-fn keyword) :text)]
       (.offer queue tweet)))
   (fn [_resp]
     (println "closing connection..."))
   (fn [_resp ex]
     (.printStackTrace ex))))

(defspout twitter-spout ["tweet"]
  [conf context collector]
  (let [queue (ConcurrentLinkedQueue.)
        sampler (statuses-sample :oauth-creds my-creds
                                 :callbacks (make-callback queue))]
    (spout
     (close []
            ((:cancel (meta sampler))))
     (nextTuple []
                (if-let [tweet (.poll queue)]
                  (emit-spout! collector [tweet])
                  (Thread/sleep 1))))))
