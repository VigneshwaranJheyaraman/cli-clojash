#!/usr/bin/env bb

(require '[babashka.deps :as deps]
         '[babashka.pods :as pods])

(deps/add-deps '{:deps {org.clojure/clojure {:mvn/version "1.10.3"}}})

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[clojure.tools.logging :as log])

(import '(java.io IOException))

(defn- hash-it
  "I serialize data structures and return hash of it"
  [data]
  (let [separator ","
        serialize #(apply str (concat (interpose separator
                                                 %))
                          ["\n"])]
    (-> (cond
          (map? data)    (serialize (vals data)) 
          (coll? data)   (serialize data)
          (string? data) data
          :else          (str data))
        hash)))

(defn find-missing
  "Compare CSV files and finds the missing columns"
  [source-file what-we-have-file columns-to-compare]
  (let [extract-col #(select-keys % columns-to-compare)
        find-value  (comp hash-it
                          extract-col)
        we-have-it? (->> (map find-value
                              source-file)
                         set)]
    (->> (filter (comp not
                       we-have-it?
                       find-value)
                 what-we-have-file)
         (map extract-col))))

(defn read-csv
  "Reads the csv content and returns them"
  [location]
  (with-open [csv-file (io/reader location)]
    (let [csv-data       (doall (csv/read-csv csv-file))
          csv-columns    (map #(some-> % str/trim keyword) (first csv-data))
          csv-rows       (rest csv-data)
          transform-rows (fn [index row-value]
                           [(nth csv-columns index) (some-> row-value str/trim)])]
      (map (comp (partial apply hash-map)
                 (partial apply concat)
                 (partial map-indexed transform-rows))
           csv-rows))))


(defn -main
  "public static void main(String args)"
  []
  (let [[complete-file-loc
         incomplete-file-loc] (take 2 *command-line-args*)]
    (try
      (let [complete-data   (read-csv complete-file-loc)
            incomplete-data (read-csv incomplete-file-loc)]
        (find-missing complete-data
                      incomplete-data
                      #{:Id}))
      (catch IOException e
        (log/errorf "Invalid file details provided: %s" (.getMessage e))
        (System/exit -1))
      (catch Exception e
        (log/errorf "Unhandled Exception: %s" (.getMessage e))
        (System/exit -1)))))

(-main)
