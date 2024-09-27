pub fn refnum_to_text(refnum: u64) -> String {
    if refnum == 0 {
        return "NONE".to_owned();
    }

    let text = refnum.to_string();
    let (code, rest) = text.split_at(1);

    if code == "1" {
        // Guide Star Catalog (GSC)
        let mut r = String::with_capacity(rest.len());
        let (front, back) = rest.split_at(1);

        if front == "1" {
            r.push('N');
            r.push_str(back);
            return r;
        }

        if front == "2" {
            r.push('S');
            r.push_str(back);
            return r;
        }
    } else if code == "2" {
        // Kepler Input Catalog
        let mut r = String::with_capacity(text.len());
        r.push('K');
        r.push_str(rest);
        return r;
    } else if code == "3" || code == "4" {
        // 3: "DASCH" - transients / new sources??
        // 4: APASS DR8
        // There is probably a less-dumb way to do this.
        let mut bad = false;
        let mut r = String::with_capacity(21);

        if text.len() != 15 {
            bad = true;
        } else {
            if code == "3" {
                r.push_str("DASCH_J");
            } else {
                r.push_str("APASS_J");
            }

            let (front, back) = rest.split_at(6);
            r.push_str(front);
            r.push('.');
            let (front, back) = back.split_at(1);
            r.push_str(front);
            let (front, back) = back.split_at(1);

            if front.starts_with('1') {
                r.push('+');
            } else if front.starts_with('2') {
                r.push('-');
            } else {
                bad = true;
            }

            r.push_str(back);
        }

        if bad {
            return "MALFORMED-DASCH/APASS".to_owned();
        } else {
            return r;
        }
    } else if code == "5" {
        // Tycho 2
        let mut r = String::with_capacity(text.len());
        r.push('T');
        r.push_str(rest);
        return r;
    } else if code == "6" {
        // UCAC-4
        let mut r = String::with_capacity(text.len());
        r.push('U');
        r.push_str(rest);
        return r;
    } else if code == "7" {
        return "UNHANDLED-GAIA1".to_owned();
    } else if code == "8" {
        return "UNHANDLED-GAIA2".to_owned();
    } else if code == "9" {
        // ATLAS-refcat2
        let mut r = String::with_capacity(text.len() + 6);
        r.push_str("ATLAS2_");
        r.push_str(rest);
        return r;
    }

    "UNKNOWN".to_owned()
}
