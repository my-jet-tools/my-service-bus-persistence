use my_service_bus::shared::sub_page::SubPageId;

#[derive(Clone, Copy)]
pub struct ArchiveFileNo(i64);

impl ArchiveFileNo {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn from_sub_page_id(sub_page_id: SubPageId) -> Self {
        Self(sub_page_id.get_value() / super::consts::ARCHIVE_SUB_PAGES_PER_FILE as i64)
    }

    pub fn get_value(&self) -> i64 {
        self.0
    }

    pub fn as_ref(&self) -> &i64 {
        &self.0
    }

    pub fn get_first_sub_page_id(&self) -> SubPageId {
        let result = self.get_value() * super::consts::ARCHIVE_SUB_PAGES_PER_FILE as i64;
        SubPageId::new(result)
    }

    pub fn get_toc_offset(&self, sub_page_id: SubPageId) -> usize {
        let result = (sub_page_id.get_value() - self.get_first_sub_page_id().get_value())
            * super::consts::TOC_STRUCTURE_SIZE as i64;

        result as usize
    }

    pub fn get_file_name(&self) -> String {
        format!("{:019}.archive", self.get_value())
    }
}

impl Into<ArchiveFileNo> for SubPageId {
    fn into(self) -> ArchiveFileNo {
        ArchiveFileNo::from_sub_page_id(self)
    }
}

#[cfg(test)]
mod tests {

    use my_service_bus::shared::sub_page::SubPageId;

    use crate::archive_storage::ArchiveFileNo;

    #[test]
    fn get_file_names() {
        assert_eq!(
            0,
            ArchiveFileNo::from_sub_page_id(SubPageId::new(0)).get_value()
        );

        assert_eq!(
            0,
            ArchiveFileNo::from_sub_page_id(SubPageId::new(1)).get_value()
        );

        assert_eq!(
            0,
            ArchiveFileNo::from_sub_page_id(SubPageId::new(9_999)).get_value()
        );

        assert_eq!(
            1,
            ArchiveFileNo::from_sub_page_id(SubPageId::new(10_000)).get_value()
        );
    }

    #[test]
    fn test_offsets() {
        let file_no = ArchiveFileNo::from_sub_page_id(SubPageId::new(0));
        assert_eq!(0, file_no.get_toc_offset(SubPageId::new(0)));
        assert_eq!(12, file_no.get_toc_offset(SubPageId::new(1)));
        assert_eq!(24, file_no.get_toc_offset(SubPageId::new(2)));
        assert_eq!(119_988, file_no.get_toc_offset(SubPageId::new(9_999)));

        let file_no = ArchiveFileNo::from_sub_page_id(SubPageId::new(10_000));
        assert_eq!(0, file_no.get_toc_offset(SubPageId::new(10_000)));
        assert_eq!(12, file_no.get_toc_offset(SubPageId::new(10_001)));
        assert_eq!(24, file_no.get_toc_offset(SubPageId::new(10_002)));
        assert_eq!(119_988, file_no.get_toc_offset(SubPageId::new(19_999)));

        let file_no = ArchiveFileNo::from_sub_page_id(SubPageId::new(20_000));
        assert_eq!(0, file_no.get_toc_offset(SubPageId::new(20_000)));
        assert_eq!(12, file_no.get_toc_offset(SubPageId::new(20_001)));
        assert_eq!(24, file_no.get_toc_offset(SubPageId::new(20_002)));
        assert_eq!(119_988, file_no.get_toc_offset(SubPageId::new(29_999)));
    }
}
