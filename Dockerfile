# syntax=docker/dockerfile:1

# ── Збірка ────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /build

# Збираємо тим самим Maven, що й локально: версія закріплена обгорткою,
# тож образ не залежить від того, який maven опинився в базовому образі.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./

# Залежності окремим шаром: pom змінюється рідше за код, тож кеш живе довше.
# kite-contracts лежить у GitHub Packages, тож збірці потрібен settings.xml із
# токеном. Він приходить секретом BuildKit і в шарах образу не лишається;
# кеш підмонтований у repository, щоб не затирати сам файл.
RUN --mount=type=cache,target=/root/.m2/repository \
    --mount=type=secret,id=m2settings,target=/root/.m2/settings.xml,required=false \
    ./mvnw -B -ntp dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository \
    --mount=type=secret,id=m2settings,target=/root/.m2/settings.xml,required=false \
    ./mvnw -B -ntp clean package -DskipTests

# Шарована розкладка Spring Boot: тонкий jar і залежності лягають різними
# шарами, тож нова збірка штовхає в реєстр лише те, що справді змінилося.
# Ім'я jar-а закріплюємо — інакше воно залежить від версії й імені артефакта.
RUN java -Djarmode=tools -jar target/*.jar extract --layers --destination /extracted \
    && mv /extracted/application/*.jar /extracted/application/app.jar

# ── Запуск ────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

RUN addgroup -S kite && adduser -S -G kite kite \
    && apk add --no-cache curl
WORKDIR /app

COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./

USER kite
EXPOSE 8080

# Ліміт пам'яті контейнера — 384 МБ, купа бере 75% від нього (12.1).
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseSerialGC -XX:TieredStopAtLevel=1"

# AppCDS (12.1) тут свідомо не вмикаємо: тренувальний запуск
# (-XX:ArchiveClassesAtExit + -Dspring.context.exit=onRefresh) підіймає
# контекст, а справжньому сервісу для цього потрібна база. Коли сервіс
# навчиться підіматися без неї, додати два рядки — і старт стане швидшим.

HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=5 \
    CMD curl -fsS http://localhost:8080/actuator/health/readiness || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
