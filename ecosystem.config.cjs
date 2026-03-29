module.exports = {
  apps: [
    {
      name: 'ayrweather',
      script: 'uvicorn',
      args: 'server:app --host 0.0.0.0 --port 3875',
      interpreter: '/Users/noc/operations/ayrweather/.venv/bin/python',
      cwd: '/Users/noc/operations/ayrweather',
      env: {
        PYTHONUNBUFFERED: '1',
      },
      watch: false,
      autorestart: true,
      max_restarts: 10,
      min_uptime: '10s',
    },
  ],
};
