//! Independent processing and export scopes for an ordered module list.

/// Processing may resume partway through a source while export covers it all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleSelection<M> {
    pub processing_modules: Vec<M>,
    pub export_modules: Vec<M>,
}

impl<M: Copy + PartialEq> ModuleSelection<M> {
    /// Selects from the caller's ordered active list without discovering modules.
    ///
    /// An explicit module without resume is allowed even if inactive. Resume
    /// starts at the first matching active module; an absent match produces no
    /// processing work, retaining the entire active export scope. Without an
    /// explicit module, `resume` has no effect. Input order is preserved.
    ///
    /// ```
    /// use sctys_rust_utilities::pipeline::ModuleSelection;
    /// let selection = ModuleSelection::new(vec!['A', 'B', 'C'], Some('B'), true);
    /// assert_eq!(selection.processing_modules, vec!['B', 'C']);
    /// assert_eq!(selection.export_modules, vec!['A', 'B', 'C']);
    /// ```
    pub fn new(active_modules: Vec<M>, input_module: Option<M>, resume: bool) -> Self {
        let processing_modules = match input_module {
            Some(module) if resume => active_modules
                .iter()
                .copied()
                .skip_while(|candidate| *candidate != module)
                .collect(),
            Some(module) => vec![module],
            None => active_modules.clone(),
        };
        let export_modules = match input_module {
            Some(module) if !resume => vec![module],
            _ => active_modules,
        };
        Self {
            processing_modules,
            export_modules,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selection_scopes() {
        for (input, resume, processing, export) in [
            (None, false, vec!['A', 'B', 'C'], vec!['A', 'B', 'C']),
            (None, true, vec!['A', 'B', 'C'], vec!['A', 'B', 'C']),
            (Some('B'), false, vec!['B'], vec!['B']),
            (Some('B'), true, vec!['B', 'C'], vec!['A', 'B', 'C']),
            (Some('A'), true, vec!['A', 'B', 'C'], vec!['A', 'B', 'C']),
            (Some('C'), true, vec!['C'], vec!['A', 'B', 'C']),
            (Some('D'), false, vec!['D'], vec!['D']),
            (Some('D'), true, vec![], vec!['A', 'B', 'C']),
        ] {
            let selection = ModuleSelection::new(vec!['A', 'B', 'C'], input, resume);
            assert_eq!(selection.processing_modules, processing);
            assert_eq!(selection.export_modules, export);
        }
    }

    #[test]
    fn test_empty_active_list() {
        for resume in [false, true] {
            let all = ModuleSelection::<char>::new(vec![], None, resume);
            assert!(all.processing_modules.is_empty());
            assert!(all.export_modules.is_empty());
            let explicit = ModuleSelection::new(vec![], Some('A'), resume);
            let expected = if resume { vec![] } else { vec!['A'] };
            assert_eq!(explicit.processing_modules, expected);
            assert_eq!(explicit.export_modules, expected);
        }
    }

    #[test]
    fn test_resume_preserves_order_and_duplicates() {
        let selection = ModuleSelection::new(vec!['C', 'B', 'A', 'B'], Some('B'), true);
        assert_eq!(selection.processing_modules, vec!['B', 'A', 'B']);
        assert_eq!(selection.export_modules, vec!['C', 'B', 'A', 'B']);
    }
}
