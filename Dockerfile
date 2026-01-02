# Stage 1: Builder
FROM node:22-alpine AS builder

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy package files (including pnpm-lock.yaml)
COPY package.json pnpm-lock.yaml ./

# Install ALL dependencies (including devDependencies for build)
RUN pnpm install --frozen-lockfile

# Copy source code and tests (needed for spec files during build)
COPY tsconfig*.json nest-cli.json ./
COPY src ./src
COPY test ./test

# Build the application
RUN pnpm run build

# Stage 2: Production
FROM node:22-alpine AS production

WORKDIR /app

# Install curl for health checks and pnpm
RUN apk add --no-cache curl && \
    corepack enable && corepack prepare pnpm@latest --activate

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nestjs -u 1001

# Copy package files
COPY package.json pnpm-lock.yaml ./

# Install only production dependencies
RUN pnpm install --prod --frozen-lockfile && \
    pnpm store prune

# Copy built application from builder
COPY --from=builder --chown=nestjs:nodejs /app/dist ./dist

# Copy README.md for root endpoint
COPY --chown=nestjs:nodejs README.md ./

# Switch to non-root user
USER nestjs

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:3000/v1/health || exit 1

# Start the application
CMD ["node", "dist/src/main.js"]
