# Wallet UI

React/Vite интерфейс демо-кошелька. Первый экран должен быть рабочим приложением, не landing page.

## Назначение

- Показывает баланс, последние транзакции, форму создания операции и экран сигналов поведения.
- Отправляет пользовательские операции в wallet API.
- Собирает clickstream: переходы, page dwell time, клики, движения курсора с throttling.

## Требования

- Сохраняй стек React + TypeScript + Vite.
- Не добавляй отдельный frontend state framework без явной необходимости.
- API-вызовы должны идти через относительные `/api/...` routes, чтобы Nginx proxy в контейнере работал без внешней настройки.
- Clickstream batching должен сохранять `session_id` между перезагрузками через `localStorage`.
- Движения курсора должны оставаться throttled, чтобы не перегружать Kafka.
- UI должен быть рабочим на desktop и mobile; не добавляй текст, который описывает внутреннюю механику приложения вместо полезного интерфейса.
- Используй `lucide-react` для иконок, если нужна новая иконка.

## Проверки

- `docker compose -f infra/docker-compose.yml --env-file .env.example build wallet-ui`
- При локальной разработке: `npm install`, затем `npm run build` из `apps/wallet-ui`.

