;;;
;;; Copyright (C) 2008-2009 Keith James. All rights reserved.
;;;
;;; This program is free software: you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation, either version 3 of the License, or
;;; (at your option) any later version.
;;;
;;; This program is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with this program.  If not, see <http://www.gnu.org/licenses/>.
;;;

(in-package :tokyo-cabinet)

(defmethod initialize-instance :after ((db tc-bdb) &key)
  (with-slots (ptr) db
    (setf ptr (tcbdbnew))))

(defmethod set-comparator ((db tc-bdb) (comparator symbol))
  (tcbdbsetcmpfunc (ptr-of db) (or (%builtin-comparator comparator)
                                   comparator) (null-pointer)))

(defmethod raise-error ((db tc-bdb) &optional text)
  (let* ((code (tcbdbecode (ptr-of db)))
         (msg (tcbdberrmsg code)))
    (error 'dbm-error :error-code code :error-msg msg :text text)))

(defmethod maybe-raise-error ((db tc-bdb) &optional text)
  (let ((ecode (tcbdbecode (ptr-of db))))
    (cond ((= +tcesuccess+ ecode)
           t)
          ((= +tcenorec+ ecode)
           nil)
          (t
           (raise-error db text)))))

(defmethod dbm-open ((db tc-bdb) filespec &rest mode)
  (let ((db-ptr (ptr-of db)))
    (validate-open-mode mode)
    (unless (tcbdbopen db-ptr filespec mode) ; opens db by side-effect
      (let* ((code (tcbdbecode db-ptr))
             (msg (tcbdberrmsg code)))
        (tcbdbdel db-ptr) ; clean up on error
        (error 'dbm-error :error-code code :error-msg msg))))
  db)

(defmethod dbm-close ((db tc-bdb))
  (tcbdbclose (ptr-of db)))

(defmethod dbm-delete ((db tc-bdb))
  (tcbdbdel (ptr-of db)))

(defmethod dbm-vanish ((db tc-bdb))
  (tcbdbvanish (ptr-of db)))

(defmethod dbm-begin ((db tc-bdb))
  (unless (tcbdbtranbegin (ptr-of db))
    (raise-error db)))

(defmethod dbm-commit ((db tc-bdb))
  (unless (tcbdbtrancommit (ptr-of db))
    (raise-error db)))

(defmethod dbm-abort ((db tc-bdb))
  (unless (tcbdbtranabort (ptr-of db))
    (raise-error db)))

(defmethod dbm-get ((db tc-bdb) (key string) &optional (type :string))
  (ecase type
    (:string (get-string->string db key #'tcbdbget2))
    (:octets (get-string->octets db key #'tcbdbget))))

(defmethod dbm-get ((db tc-bdb) (key integer) &optional (type :string))
  (ecase type
    (:string (get-int32->string db key #'tcbdbget))
    (:octets (get-int32->octets db key #'tcbdbget))))

(defmethod dbm-put ((db tc-bdb) (key string) (value string) 
                    &key (mode :replace))
  (put-string->string db key value (%bdb-str-put-fn mode)))

(defmethod dbm-put ((db tc-bdb) (key string) (value vector) 
                    &key (mode :replace))
  (put-string->octets db key value (%bdb-put-fn mode)))

(defmethod dbm-put ((db tc-bdb) (key integer) (value string)
                    &key (mode :replace))
  (put-int32->string db key value (%bdb-put-fn mode)))

(defmethod dbm-put ((db tc-bdb) (key integer) (value vector)
                    &key (mode :replace))
  (put-int32->octets db key value (%bdb-put-fn mode)))

(defmethod dbm-rem ((db tc-bdb) (key string) &key remove-dups)
  (if remove-dups
      (rem-string->duplicates db key #'tcbdbout3)
    (rem-string->value db key #'tcbdbout2)))

(defmethod dbm-rem ((db tc-bdb) (key integer) &key remove-dups)
  (if remove-dups
      (rem-int32->value db key #'tcbdbout3)
    (rem-int32->value db key #'tcbdbout)))

(defmethod iter-open ((db tc-bdb))
  (make-instance 'bdb-iterator :ptr (tcbdbcurnew (ptr-of db))))

(defmethod iter-close ((iter bdb-iterator))
  (tcbdbcurdel (ptr-of iter)))

(defmethod iter-first ((iter bdb-iterator))
  (tcbdbcurfirst (ptr-of iter)))

(defmethod iter-last ((iter bdb-iterator))
  (tcbdbcurlast (ptr-of iter)))

(defmethod iter-prev ((iter bdb-iterator))
  (tcbdbcurprev (ptr-of iter)))

(defmethod iter-next ((iter bdb-iterator))
  (tcbdbcurnext (ptr-of iter)))
  
(defmethod iter-jump ((iter bdb-iterator) (key string))
  (tcbdbcurjump2 (ptr-of iter) key))

(defmethod iter-jump ((iter bdb-iterator) (key integer))
  (declare (type int32 key))
  (with-foreign-object (key-ptr :int32)
      (setf (mem-ref key-ptr :int32) key)
      (tcbdbcurjump (ptr-of iter) key-ptr (foreign-type-size :int32))))

(defmethod iter-get ((iter bdb-iterator) &optional (type :string))
  (let ((value-ptr nil))
    (unwind-protect
         (with-foreign-object (size-ptr :int)
           (setf value-ptr (tcbdbcurval (ptr-of iter) size-ptr))
           (unless (null-pointer-p value-ptr)
             (ecase type
               (:string (foreign-string-to-lisp value-ptr :count
                                                (mem-ref size-ptr :int)))
               (:integer (mem-ref value-ptr :int32))
               (:octets (copy-foreign-value value-ptr size-ptr)))))
      (when (and value-ptr (not (null-pointer-p value-ptr)))
        (foreign-free value-ptr)))))

(defmethod iter-put ((iter bdb-iterator) (value string)
                     &key (mode :current))
  (tcbdbcurput2 (ptr-of iter) value (%bdb-iter-mode mode)))

(defmethod iter-put ((iter bdb-iterator) (value vector)
                     &key (mode :current))
  (declare (type (vector (unsigned-byte 8)) value))
  (let ((value-len (length value)))
    (with-foreign-object (value-ptr :unsigned-char value-len)
      (tcbdbcurput (ptr-of iter) value-ptr value-len (%bdb-iter-mode mode)))))

(defmethod iter-rem ((iter bdb-iterator))
  (tcbdbcurout (ptr-of iter)))

(defmethod iter-key ((iter bdb-iterator) &optional (type :string))
  (let ((key-ptr nil))
    (unwind-protect
         (with-foreign-object (size-ptr :int)
           (setf key-ptr (tcbdbcurkey (ptr-of iter) size-ptr))
           (unless (null-pointer-p key-ptr)
             (ecase type
               (:string (foreign-string-to-lisp key-ptr :count
                                                (mem-ref size-ptr :int)))
               (:integer (mem-ref key-ptr :int32))
               (:octets (copy-foreign-value key-ptr size-ptr)))))
      (when (and key-ptr (not (null-pointer-p key-ptr)))
        (foreign-free key-ptr)))))

(defmethod dbm-num-records ((db tc-bdb))
  (tcbdbrnum (ptr-of db)))

(defmethod dbm-file-namestring ((db tc-bdb))
  (tcbdbpath (ptr-of db)))

(defmethod dbm-file-size ((db tc-bdb))
  (tcbdbfsiz (ptr-of db)))

(defmethod dbm-optimize ((db tc-bdb) &key (leaf 0) (non-leaf 0)
                         (bucket-size 0) (rec-align -1) (free-pool -1)
                         (opts '(:defaults)))
  (tcbdboptimize (ptr-of db) leaf non-leaf bucket-size rec-align
                 free-pool opts))

(defmethod dbm-cache ((db tc-bdb) &key (leaf 1024) (non-leaf 512))
  (tcbdbsetcache (ptr-of db) leaf non-leaf))

(defun %bdb-put-fn (mode)
  (ecase mode
    (:replace #'tcbdbput)
    (:keep #'tcbdbputkeep)
    (:concat #'tcbdbputcat)
    (:duplicate #'tcbdbputdup)))

(defun %bdb-str-put-fn (mode)
  (ecase mode
    (:replace #'tcbdbput2)
    (:keep #'tcbdbputkeep2)
    (:concat #'tcbdbputcat2)
    (:duplicate #'tcbdbputdup2)))

(defun %builtin-comparator (type)
  (foreign-symbol-pointer (case type
                            (:lexical "tccmpintlexical")
                            (:decimal "tccmpintdecimal")
                            (:int32 "tccmpint32")
                            (:int64 "tccmpint64"))))

(defun %bdb-iter-mode (mode)
  (ecase mode
    (:current +bdbcpcurrent+)
    (:prev +bdbcpbefore+)
    (:after +bdbcpafter+)))
