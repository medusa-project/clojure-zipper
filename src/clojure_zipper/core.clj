(ns clojure-zipper.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [aleph.tcp :as tcp]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [manifold.stream :as ms])
  (:import (java.util.zip ZipOutputStream ZipEntry)
           (org.apache.commons.io IOUtils))
  (:gen-class))

(defn add-zip-entry [zip-stream storage-path manifest-entry]
  (let [[dash file-size content-path zip-path] (str/split manifest-entry #" " 4)
        content-path (str/replace-first content-path #"^\/internal\/" "")
        real-path (io/file storage-path content-path)]
    (.putNextEntry zip-stream (ZipEntry. zip-path))
    (with-open [input-stream (io/input-stream real-path)]
      (io/copy input-stream zip-stream))))

(defn zip [manifest-path storage-path]
  (println manifest-path)
  (let [zip-stream (ZipOutputStream. System/out)]
    (.setLevel zip-stream 0)
    (with-open [manifest-reader (io/reader manifest-path)]
      (doseq [line (line-seq manifest-reader)]
        (add-zip-entry zip-stream storage-path line)))
    (.close zip-stream)))

;;;arguments are:
;;;- path to manifest file
;;;- storage path, i.e. root path from which the content paths are given
(defn -main
  [& args]
  (let [[manifest-path storage-path] args]
    (zip manifest-path storage-path)))

;;The idea going forward - use clojure aleph and byte-streams library to make this into a TCP server local
;;to the downloader server. Accept a spec with the manifest-path, storage-path, and (possibly) manifest-format as a JSON
;;object on connection, and then create the zip stream on the output stream of that connection. Modify the Rails
;;server to do this instead of using the command line pipe call. See if that works.
;;So main will just start up the server, and on connect we'll essentially do what we in the current main.

;(defn handle-connection [stream info]
;  (println "Received connection")
;  (let [input (bs/to-string stream)
;        parameters (json/parse-string input true)
;        output (bs/to-output-stream stream)]
;    (println "Received Input: " (str input))
;    (println (:manifest-path parameters))
;    (println (:content-path parameters))))


;;I think I may need to understand Manifold streams better to get this to work!
;; (defn handle-connection [stream info]
;;   (try
;;     (println "Received Connection: " (str info))
;;     (println "Received Data:" (bs/to-string (ms/take! stream)))
;;     (catch Exception e (println e))))

;; (defn -main [& args]
;;   (tcp/start-server handle-connection {:port 19876})
;;   (while true (java.lang.Thread/sleep 3600000)))

;;It's worth considering the alternative of using an http server here instead of a tcp server, depending on what
;;I can turn up in either category. For an http server just pass the stuff as parameters. But I think they would
;;also easily take an input stream for the return body, which could be provided by a pipe input/output stream
;;pair connected to the zip stream.  Pedestal, for example, looks like it could support this.
;;On the ruby side we make the http call and we should be able to get back an IO as response.body, which we
;;copy to the response.stream as before.
