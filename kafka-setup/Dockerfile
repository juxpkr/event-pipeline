FROM confluentinc/cp-kafka:7.5.0

USER root

# Confluent의 GPG 키와 패키지 저장소를 추가하고, confluent-common 패키지(cub 포함)를 설치
RUN rpm --import https://packages.confluent.io/rpm/7.5/archive.key && \
    echo -e '[Confluent.dist]\nname=Confluent repository\nbaseurl=https://packages.confluent.io/rpm/7.5\ngpgcheck=1\ngpgkey=https://packages.confluent.io/rpm/7.5/archive.key\nenabled=1' > /etc/yum.repos.d/confluent.repo && \
    microdnf install -y confluent-common && \
    microdnf clean all

# setup-topics.sh 복사
COPY setup-topics.sh /opt/kafka/bin/setup-topics.sh
RUN chmod +x /opt/kafka/bin/setup-topics.sh

# 기본 엔트리포인트는 setup-topics.sh
ENTRYPOINT ["/opt/kafka/bin/setup-topics.sh"]