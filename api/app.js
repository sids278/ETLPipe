import express from 'express';
import swaggerUi from 'swagger-ui-express';
import routes from './routes.js';
import { swaggerSpec } from './swagger.js';

const app = express();
app.use(express.json());
app.get('/', (req, res) => {
  res.send('Hello World!');
});
app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));
app.use(routes);

// Last stop for any error thrown in a route (Express 5 forwards async errors here).
app.use((err, req, res, next) => {
  console.error(err);
  res.status(err.status ?? 500).json({ error: err.message });
});

export default app;
