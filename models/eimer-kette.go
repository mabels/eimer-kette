package models

type EimerKetteOwner struct {
	DisplayName string `parquet:"name=displayName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ID          string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type EimerKetteItem struct {
	ETag         string `parquet:"name=etag, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Key          string `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LastModified int64  `parquet:"name=lastModified, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	// Owner        EimerKetteOwner
	Size         int64  `parquet:"name=size, type=INT64, convertedtype=INT_64"`
	StorageClass string `parquet:"name=storageClass, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
