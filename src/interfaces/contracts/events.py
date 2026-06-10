"""
Принимаем от кафки

принимаем:
type EmailRequested struct {
    EventID         string    `json:"event_id"`
    Type            string    `json:"type"`
    UserID          string    `json:"user_id"`
    Email           string    `json:"email"`
    VerificationURL string    `json:"verification_url"`
    ExpiresAt       time.Time `json:"expires_at"`
    CreatedAt       time.Time `json:"created_at"`
}


"""