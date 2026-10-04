use crate::domain::value_object::CaseStatus;

/// What became of a report, as its reporter sees it (DSA Art. 16(5)). Derived
/// from the review case the report fed, never stored. Coarse on purpose: the
/// reporter learns whether action was taken, never which sanction applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportStatus {
    UnderReview,
    ActionTaken,
    NoViolation,
}

impl ReportStatus {
    /// `None`: the case is not persisted yet (the report was recorded first).
    /// An appealed case is still actioned until the appeal overturns it, which
    /// moves the case to `Dismissed`.
    pub fn of_case(status: Option<CaseStatus>) -> Self {
        match status {
            None | Some(CaseStatus::Open | CaseStatus::Triaged) => Self::UnderReview,
            Some(CaseStatus::Actioned | CaseStatus::Appealed) => Self::ActionTaken,
            Some(CaseStatus::Dismissed) => Self::NoViolation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_follows_its_case() {
        assert_eq!(ReportStatus::of_case(None), ReportStatus::UnderReview);
        assert_eq!(ReportStatus::of_case(Some(CaseStatus::Open)), ReportStatus::UnderReview);
        assert_eq!(ReportStatus::of_case(Some(CaseStatus::Triaged)), ReportStatus::UnderReview);
        assert_eq!(ReportStatus::of_case(Some(CaseStatus::Actioned)), ReportStatus::ActionTaken);
        assert_eq!(ReportStatus::of_case(Some(CaseStatus::Appealed)), ReportStatus::ActionTaken);
        assert_eq!(ReportStatus::of_case(Some(CaseStatus::Dismissed)), ReportStatus::NoViolation);
    }
}
