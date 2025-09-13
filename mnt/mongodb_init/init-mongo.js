// Criação do banco de dados e coleção
db = db.getSiblingDB('compass');
db.createCollection('reviews_instituicao_compass');

db.createUser({
  user: "root",
  pwd: "secure_password123",
  roles: [{ role: "root", db: "admin" }]
});


db.createUser({
  user: "app_user",
  pwd: "secure_password123",
  roles: [{ role: "readWriteAnyDatabase", db: "compass", db: "admin" }, "dbAdminAnyDatabase", "clusterAdmin"]
});
