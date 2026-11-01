//! An animated avatar's sprite sheet layout (#945, #948, #974): columns, cell,
//! gutter and frame rate are fixed by the rendition kind; the frame count is
//! the sheet's own (its rows × columns), read from its stored size, so a
//! 24-frame sheet baked before #974 still plays as 24 frames.

use crate::domain::value_object::RenditionKind;

/// Frames a sheet is baked with: 4 s at [`SHEET_FPS`] (#974; 24 before).
pub const SHEET_FRAMES: u32 = 48;
/// The fewest frames a sheet takes: 2 s (a shorter clip is refused).
pub const MIN_SHEET_FRAMES: u32 = 24;
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
    /// The geometry a sheet of `kind` is baked with ([`SHEET_FRAMES`]
    /// frames); `None` for any other kind.
    pub fn of(kind: RenditionKind) -> Option<Self> {
        Self::with_frames(kind, SHEET_FRAMES)
    }

    /// A sheet of `kind` holding `frame_count` frames; `None` for any other
    /// kind.
    pub fn with_frames(kind: RenditionKind, frame_count: u32) -> Option<Self> {
        let cell_px = match kind {
            RenditionKind::SheetSmall => 128,
            RenditionKind::SheetLarge => 256,
            _ => return None,
        };
        Some(Self {
            frame_count,
            columns: SHEET_COLUMNS,
            frame_duration_ms: 1000 / SHEET_FPS,
            gutter_px: SHEET_GUTTER_PX,
            cell_px,
        })
    }

    /// The geometry of a stored sheet rendition, from its `height`: every row
    /// is full, so frames = rows × columns (#974: 24 for a sheet baked before,
    /// 48 after). `None` for any other kind.
    pub fn of_rendition(kind: RenditionKind, height: u32) -> Option<Self> {
        let probe = Self::of(kind)?;
        let rows = (height / probe.cell_px).max(1);
        Self::with_frames(kind, rows * probe.columns)
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
    fn sheets_are_baked_with_48_cells_over_4_columns() {
        let large = SheetGeometry::of(RenditionKind::SheetLarge).unwrap();
        assert_eq!((large.rows(), large.size(), large.frame_duration_ms), (12, (1024, 3072), 83));
        assert_eq!(SheetGeometry::of(RenditionKind::SheetSmall).unwrap().size(), (512, 1536));
        assert_eq!(SheetGeometry::of(RenditionKind::Poster), None);
    }

    /// #974: a stored sheet says its own frame count by its height, so a
    /// 24-frame sheet from before still plays as 24.
    #[test]
    fn a_stored_sheet_reads_its_frames_from_its_height() {
        assert_eq!(SheetGeometry::of_rendition(RenditionKind::SheetLarge, 1536).unwrap().frame_count, 24);
        assert_eq!(SheetGeometry::of_rendition(RenditionKind::SheetLarge, 3072).unwrap().frame_count, 48);
        assert_eq!(SheetGeometry::of_rendition(RenditionKind::SheetSmall, 768).unwrap().frame_count, 24);
        assert_eq!(SheetGeometry::of_rendition(RenditionKind::SheetSmall, 1536).unwrap().frame_count, 48);
        assert_eq!(SheetGeometry::of_rendition(RenditionKind::Poster, 256), None);
    }
}
