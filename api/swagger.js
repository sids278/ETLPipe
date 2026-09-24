import { fileURLToPath } from 'node:url';
import swaggerJsdoc from 'swagger-jsdoc';

// Resolve the routes folder from this file, so Swagger works no matter where node is started from.
const routesGlob = fileURLToPath(new URL('./routes/', import.meta.url)) + '*.js';

export const swaggerSpec = swaggerJsdoc({
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Pipeline Load-Test API',
      version: '1.0.0',
      description: 'Health checks, schema setup, data seeding and SQL Server to ClickHouse sync runs.',
    },
  },
  apis: [routesGlob],
});
