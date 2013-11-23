(ns glutton.spouts
  "Spouts.

More info on the Clojure DSL here:

https://github.com/nathanmarz/storm/wiki/Clojure-DSL"
  (:require [backtype.storm [clojure :refer [defspout spout emit-spout!]]]))

(defspout type-spout ["type"]
  [conf context collector]
  (let [stormys [:regular :bizarro]]
    (spout
     (nextTuple []
       (Thread/sleep 1000)
       (emit-spout! collector [(rand-nth stormys)]))
     (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))

(def sample-tweets
  ["Sencha fresh mint tea on a Yahoo Oktoberfest beer jar with a mince pie. #globalculture pic.twitter.com/twHmqqRvwB"
   "but 4real tho, who thought fusion earrings would be more stable than the fusion dance!?! Amiright!?"
   "As argument for why Android doesn't use SysV IPC, they include source for a linux exploit in the NDK docs. :-)"
   "Just so I don't feel left out #XboxOne pic.twitter.com/xU9zol1tmN"
   "Wouldn't it be nice to have a list of all the #XboxOne voice commands? We thought so too. http://xbx.lv/1ccfTb6"
   "Manipulate that vortex and run, you clever girl! #DoctorWho50th #DayoftheDoctor"
   "#1DDayLive HARRY WOULD SING WRECKING BALL R U KIDDING ME AND LIAM GOING WITH JT"
   "My limited edition #harrypotter stamps have finally arrived! #itsagoodday http://instagram.com/p/hEhEXNRyT0/"
   "As requested, I wrote down some of the very basic usages of cravendb in some docs http://robashton.github.io/cravendb/  #clojure"])


(defspout mock-twitter-spout ["tweet"]
  [conf context collector]
  (spout
    (nextTuple []
               (Thread/sleep 1000)
               (emit-spout! collector [(rand-nth sample-tweets)]))
    (ack [id]
         ;; You only need to define this method for reliable spouts
         ;; (such as one that reads off of a queue like Kestrel)
         ;; This is an unreliable spout, so it does nothing here
         )))
