package com.example.cardstatus;

import org.springframework.data.jpa.repository.JpaRepository;

public interface CardStatusRepository extends JpaRepository<CardStatusRecord, Long> {
}
