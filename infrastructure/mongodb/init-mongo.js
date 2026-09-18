// MongoDB initialization script for Chronos
db = db.getSiblingDB('chronos');

// Create collections used by the services
db.createCollection('users');
db.createCollection('workflows');
db.createCollection('workflow_executions');
db.createCollection('task_executions');
db.createCollection('scheduler_state');
db.createCollection('outbox_messages');

print('Collections created successfully');

// Indexes are created by the services themselves (spring.data.mongodb.auto-index-creation)
// from the @Indexed/@CompoundIndex annotations on the domain classes. Creating the same
// index keys here under different names would make the services fail at startup
// (MongoDB error 85, IndexOptionsConflict).
//
// Upgrading from an older volume that still has indexes from a previous version of this
// script? Drop the conflicting ones once:
//   db.users.dropIndex("email_1"); db.workflows.dropIndex("ownerId_1");
// or start from scratch with: docker compose down -v

print('MongoDB initialization completed successfully');
