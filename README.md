# ArangoDB
За основу данной программы взят драйвер ArangoDB на Java, который будет немного изменён, 
чтобы можно было параллельно выполнять операции вставки/удаления/обновления/замещения в ArangoDB и в другую базу данных.
Цель: если вдруг главная база данных (в моем случае ArangoDB) упадет, то данные можно восстановить, досточно извлечь их из
дополнительной базы данных

# Мои обновления
arangodb-java-driver-master/src/main/java/com/arangodb/internal/ArangoCollectionImpl2.java - изменена функция insertDocument(T value), теперь документ вставляется в базу данных и отправляется продюсером в kafka 

arangodb-java-driver-master/src/main/java/kafka/ - здесь два класса SampleConsumer.java - читатель сообщений, SampleProducer.java - отправитель сообщений
