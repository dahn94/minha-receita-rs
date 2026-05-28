use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("CNPJ inválido: {0}")]
    InvalidCnpj(String),

    #[error("período inválido (esperado YYYY-MM): {0}")]
    InvalidPeriod(String),

    #[error("arquivo de dado ausente: {0}")]
    MissingData(String),

    #[error("HTTP {status}: {url}")]
    Http { url: String, status: u16 },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
