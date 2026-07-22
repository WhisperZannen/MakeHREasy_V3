import unittest

from modules.core_personnel import (
    build_personnel_change_tags,
    rebuild_history_change_type,
)


class PersonnelHistoryLabelTest(unittest.TestCase):
    def test_status_only_does_not_create_rank_tag(self):
        old = {
            "status": "在职", "dept_id": 18, "pos_id": 35,
            "tech_grade": "T1", "post_rank": 11,
            "post_grade": "B", "old_pos_name": "研发工程师-应用",
        }
        employee = {
            "status": "离职", "dept_id": 18.0,
            "post_rank": 11.0, "post_grade": "B",
        }
        profile = {"pos_id": 35.0, "tech_grade": "T1"}

        self.assertEqual(
            build_personnel_change_tags(old, employee, profile),
            ["变为离职"],
        )

    def test_real_rank_change_is_kept(self):
        old = {
            "status": "在职", "dept_id": 18, "pos_id": 35,
            "tech_grade": "T1", "post_rank": 11,
            "post_grade": "B", "old_pos_name": "研发工程师-应用",
        }
        employee = {
            "status": "离职", "dept_id": 18,
            "post_rank": 12, "post_grade": "B",
        }
        profile = {"pos_id": 35, "tech_grade": "T1"}

        self.assertEqual(
            build_personnel_change_tags(old, employee, profile),
            ["变为离职", "岗级调整"],
        )

    def test_historical_false_rank_tag_is_removed(self):
        row = {
            "change_type": "变为离职 + 岗级调整",
            "old_dept_id": 18, "new_dept_id": 18,
            "old_pos_id": 35, "new_pos_id": 35,
            "old_pos_name": "研发工程师-应用",
            "old_tech_grade": "T1", "new_tech_grade": "T1",
            "old_post_rank": 11, "new_post_rank": 11.0,
            "old_post_grade": "B", "new_post_grade": "B",
        }
        self.assertEqual(rebuild_history_change_type(row), "变为离职")

    def test_other_real_changes_are_preserved(self):
        row = {
            "change_type": "跨部门调动 + 岗级调整",
            "old_dept_id": 12, "new_dept_id": 6,
            "old_pos_id": 25, "new_pos_id": 25,
            "old_pos_name": "研发工程师",
            "old_tech_grade": "T1", "new_tech_grade": "T1",
            "old_post_rank": 11, "new_post_rank": 11,
            "old_post_grade": "A", "new_post_grade": "A",
        }
        self.assertEqual(rebuild_history_change_type(row), "跨部门调动")

    def test_first_assignment_labels_are_not_rewritten_as_normal_transfers(self):
        row = {
            "change_type": "首次岗位分配",
            "old_dept_id": 22, "new_dept_id": 22,
            "old_pos_id": 1, "new_pos_id": 45,
            "old_pos_name": "无岗位",
            "old_tech_grade": "", "new_tech_grade": "",
            "old_post_rank": 11, "new_post_rank": 11,
            "old_post_grade": "E", "new_post_grade": "E",
        }
        self.assertEqual(rebuild_history_change_type(row), "首次岗位分配")


if __name__ == "__main__":
    unittest.main()
