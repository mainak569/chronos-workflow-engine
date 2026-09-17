// MongoDB initialization script for Chronos

db = db.getSiblingDB('chronos');

// Create collections
db.createCollection('users');
db.createCollection('workflows');
db.createCollection('executions');
db.createCollection('tasks');
db.createCollection('workers');

print('Collections created successfully');

// Create indexes for users collection
db.users.createIndex({ email: 1 }, { unique: true });
print('Users indexes created');

// Create indexes for workflows collection
db.workflows.createIndex({ ownerId: 1 });
db.workflows.createIndex({ name: 1, ownerId: 1 });
db.workflows.createIndex({ createdAt: -1 });
print('Workflows indexes created');

// Create indexes for executions collection
db.executions.createIndex({ workflowId: 1 });
db.executions.createIndex({ ownerId: 1, createdAt: -1 });
db.executions.createIndex({ status: 1 });
db.executions.createIndex({ correlationId: 1 });
print('Executions indexes created');

// Create indexes for tasks collection
db.tasks.createIndex({ executionId: 1 });
db.tasks.createIndex({ executionId: 1, taskId: 1 }, { unique: true });
db.tasks.createIndex({ status: 1 });
db.tasks.createIndex({ workerId: 1 });
db.tasks.createIndex({ nextRetryAt: 1 }, { sparse: true });
db.tasks.createIndex({ status: 1, dependenciesMet: 1 });
print('Tasks indexes created');

// Create indexes for workers collection
db.workers.createIndex({ status: 1 });
db.workers.createIndex({ lastHeartbeatAt: -1 });
print('Workers indexes created');

print('MongoDB initialization completed successfully');
