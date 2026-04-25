---
title: "Paddle vs Stripe"
type: decision
status: active
owner: pranav@startup.com
decision: "We chose Paddle over Stripe because international tax handling was easier for our current stage."
date: 2026-04-01
confidence: 0.9
source-of-truth: true
reviewed: 2026-04-01
next-review: 2026-07-01
alternatives:
  - option: Stripe
    pros:
      - More developer-friendly
      - Larger ecosystem
    cons:
      - Tax compliance is manual
      - International complexity
  - option: Paddle
    pros:
      - Built-in tax handling
      - Merchant of record
    cons:
      - Smaller ecosystem
      - Less customizable
impact: "Reduced tax compliance burden, faster international launch"
reversal-conditions: "Revisit if we need advanced payment flows or Paddle pricing becomes uncompetitive"
linked-docs:
  - concepts/payments.md
tags: [payments, billing, decision]
---

# Paddle vs Stripe

## Context
We needed a payment provider that handles international tax compliance out of the box.

## Decision
We chose Paddle over Stripe because international tax handling was easier for our current stage.

## Consequences
- Faster time to market for international customers
- Less tax compliance overhead
- Traded customization for simplicity
