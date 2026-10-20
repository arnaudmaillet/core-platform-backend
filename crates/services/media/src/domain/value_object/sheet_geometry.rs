//! An animated avatar's sprite sheet layout (#945, #948): fixed for every sheet,
//! so it is derived from the rendition kind, never stored.

use crate::domain::value_object::RenditionKind;

/// Frames in a sheet: 2 s at [`SHEET_FPS`].
pub const SHEET_FRAMES: u32 = 24;
/// Frames per second the sheet plays at.
pub const SHEET_FPS: u32 = 12;
/// Cells per row; rows = frames / columns.
pub const SHEET_COLUMNS: u32 = 4;
/// Transparent margin inside each cell, on every side.
pub const SHEET_GUTTER_PX: u32 = 2;

/// How a sheet is laid out: `frame_count` square cells of `cell_px`, row by
/// row over `columns`, each frame drawn inside a `gutter_px` transparent
/// margin, played at `frame_duration_ms` a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SheetGeometry {
    pub frame_count:       u32,
    pub columns:           u32,
    pub frame_duration_ms: u32,
    pub gutter_px:         u32,
    pub cell_px:           u32,
}

impl SheetGeometry {
    /// The geometry of a sheet rendition; `None` for any other kind.
    pub fn of(kind: RenditionKind) -> Option<Self> {
        let cell_px = match kind {
            RenditionKind::SheetSmall => 128,
            RenditionKind::SheetLarge => 256,
            _ => return None,
        };
        Some(Self {
            frame_count: SHEET_FRAMES,
            columns: SHEET_COLUMNS,
            frame_duration_ms: 1000 / SHEET_FPS,
            gutter_px: SHEET_GUTTER_PX,
            cell_px,
        })
    }

    pub fn rows(&self) -> u32 {
        self.frame_count.div_ceil(self.columns)
    }

    /// The sheet's size in pixels.
    pub fn size(&self) -> (u32, u32) {
        (self.columns * self.cell_px, self.rows() * self.cell_px)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sheets_are_24_cells_over_4_columns() {
        let large = SheetGeometry::of(RenditionKind::SheetLarge).unwrap();
        assert_eq!((large.rows(), large.size(), large.frame_duration_ms), (6, (1024, 1536), 83));
        assert_eq!(SheetGeometry::of(RenditionKind::SheetSmall).unwrap().size(), (512, 768));
        assert_eq!(SheetGeometry::of(RenditionKind::Poster), None);
    }
}
